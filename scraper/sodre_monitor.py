#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sodré Santoro Monitor - Sistema de Detecção de Lances

ARQUIVO: scraper/sodre_monitor.py

FUNCIONAMENTO:
1. Carrega TODOS os itens ativos do banco (view vw_auctions_unified)
2. Intercepta dados da API do site Sodré Santoro via Playwright
3. Cruza por LINK (chave única)
4. Atualiza tabelas base (bid_actual, bid_has_bid, lot_visits, last_scraped_at)
5. Salva histórico temporal (auction_bid_history)
6. Detecta aumentos súbitos de lances

ESTRATÉGIA:
- Interceptação Passiva (não é engenharia reversa)
- Escuta as respostas que o site já faz naturalmente
- Zero requisições extras, 100% seguro e não detectável
"""

import asyncio
import os
import sys
from datetime import datetime
from playwright.async_api import async_playwright
from supabase import create_client, Client

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# URLs para monitorar (Sodré Santoro - Veículos)
SODRE_URLS = [
    "https://www.sodresantoro.com.br/veiculos/lotes?sort=auction_date_init_asc",
]

# Critérios para detectar "itens quentes"
HOT_ITEM_THRESHOLD_VALUE = 1000  # R$ 1.000 de aumento
HOT_ITEM_THRESHOLD_PERCENT = 20  # 20% de aumento


# ============================================================================
# CLASSE PRINCIPAL
# ============================================================================

class SodreMonitor:
    """Monitor de lances Sodré Santoro com detecção de padrões"""
    
    def __init__(self):
        """Inicializa conexões e cache"""
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("❌ SUPABASE_URL e SUPABASE_KEY devem estar definidas")
        
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Cache: {link: {category, source, external_id, lot_number, prev_bid, prev_visits}}
        self.db_items = {}
        
        # Dados capturados da API: {link: lot_data}
        self.api_lots = {}
    
    # ========================================================================
    # ETAPA 1: CARREGAR DADOS DO BANCO
    # ========================================================================
    
    def load_database_items(self):
        """
        Carrega TODOS os itens Sodré Santoro ativos do banco.
        Usa paginação para suportar grandes volumes.
        """
        print("📥 Carregando itens do banco (Sodré Santoro ativos)...")
        
        try:
            page_size = 1000
            offset = 0
            total_loaded = 0
            
            while True:
                response = self.supabase.schema("auctions").table("vw_auctions_unified")\
                    .select("link,category,source,external_id,lot_number")\
                    .eq("source", "sodre")\
                    .eq("is_active", True)\
                    .range(offset, offset + page_size - 1)\
                    .execute()
                
                if not response.data:
                    break
                
                for item in response.data:
                    link = item.get("link")
                    if link:
                        self.db_items[link] = {
                            "category": item.get("category"),
                            "source": item.get("source"),
                            "external_id": item.get("external_id"),
                            "lot_number": item.get("lot_number"),
                            "prev_bid": 0,  # Será comparado com dados da API
                            "prev_visits": 0,  # Será comparado com dados da API
                        }
                
                total_loaded += len(response.data)
                print(f"   → Carregados {total_loaded} itens...")
                
                if len(response.data) < page_size:
                    break
                
                offset += page_size
            
            print(f"✅ {len(self.db_items)} itens Sodré carregados da view\n")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao carregar itens: {e}\n")
            return False
    
    # ========================================================================
    # ETAPA 2: INTERCEPTAR DADOS DA API (PLAYWRIGHT)
    # ========================================================================
    
    async def intercept_sodre_data(self):
        """
        Intercepta dados da API Sodré usando Playwright.
        
        ESTRATÉGIA:
        - Abre navegador real (headless)
        - Deixa o site carregar normalmente
        - Intercepta respostas de /api/search-lots
        - Pagina automaticamente até pegar todos os lotes
        """
        print("🌐 Iniciando interceptação Playwright...\n")
        
        all_lots = []
        
        async with async_playwright() as p:
            # Navegador headless (invisível) para CI/CD
            browser = await p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                locale='pt-BR'
            )
            
            # Anti-detecção
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)
            
            page = await context.new_page()
            
            # Função que captura respostas da API
            async def intercept_response(response):
                """Escuta passivamente as respostas do site"""
                try:
                    if '/api/search-lots' in response.url and response.status == 200:
                        data = await response.json()
                        
                        # Verifica se tem lotes (não só agregações)
                        per_page = data.get('perPage', 0)
                        
                        if per_page > 0:
                            # Extrai lotes (suporta diferentes estruturas)
                            results = data.get('results', [])
                            hits = data.get('hits', {}).get('hits', [])
                            
                            if results:
                                all_lots.extend(results)
                                print(f"   ✓ Capturados {len(results)} lotes (results)")
                            elif hits:
                                extracted = [hit.get('_source', hit) for hit in hits]
                                all_lots.extend(extracted)
                                print(f"   ✓ Capturados {len(hits)} lotes (hits)")
                
                except Exception:
                    pass  # Ignora erros de parse
            
            # Registra interceptador
            page.on('response', intercept_response)
            
            # Navega nas URLs configuradas
            for url in SODRE_URLS:
                try:
                    print(f"📄 Carregando: {url.split('?')[0]}...")
                    await page.goto(url, wait_until="networkidle", timeout=60000)
                    await asyncio.sleep(3)
                    
                    # Paginação automática (até 30 páginas)
                    for page_num in range(2, 31):
                        try:
                            # Scroll suave
                            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            await asyncio.sleep(1)
                            
                            # Procura botão "próxima página"
                            selectors = [
                                'button[aria-label*="next"]',
                                'button[aria-label*="próxim"]',
                                '.pagination button:last-child:not([disabled])',
                                'button:has-text(">")',
                                '[data-testid="next-page"]',
                            ]
                            
                            clicked = False
                            for selector in selectors:
                                try:
                                    button = page.locator(selector).first
                                    if await button.count() > 0:
                                        is_disabled = await button.get_attribute('disabled')
                                        if is_disabled is None:
                                            await button.click()
                                            await asyncio.sleep(3)
                                            clicked = True
                                            break
                                except:
                                    continue
                            
                            if not clicked:
                                print(f"   ℹ️  Fim da paginação (página {page_num-1})")
                                break
                        
                        except Exception:
                            break
                
                except Exception as e:
                    print(f"   ⚠️ Erro ao carregar URL: {e}")
            
            await browser.close()
        
        # Indexa lotes por link (chave única)
        for lot in all_lots:
            # Suporta diferentes estruturas de ID
            lot_id = lot.get('lot_id') or lot.get('id')
            if lot_id:
                link = f"https://www.sodresantoro.com.br/veiculos/lote/{lot_id}"
                self.api_lots[link] = lot
        
        print(f"\n✅ {len(self.api_lots)} lotes únicos capturados da API\n")
        return len(self.api_lots) > 0
    
    # ========================================================================
    # ETAPA 3: CRUZAR DADOS (DB ↔ API)
    # ========================================================================
    
    def cross_reference_data(self):
        """
        Cruza dados do banco com dados da API.
        
        RETORNA:
        - matched_records: lista de registros com dados completos
        - hot_items: lista de itens com aumento súbito de lances
        """
        print("🔗 Cruzando dados (DB ↔ API)...\n")
        
        matched_records = []
        hot_items = []
        
        for link, db_data in self.db_items.items():
            api_data = self.api_lots.get(link)
            
            if not api_data:
                continue  # Lote não encontrado na API
            
            # Extrai dados atuais da API
            current_bid = float(api_data.get('bid_actual') or 0)
            has_bid = api_data.get('bid_has_bid', False)
            visits = int(api_data.get('lot_visits') or 0)
            
            # Calcula variações
            prev_bid = db_data['prev_bid']
            bid_increase = current_bid - prev_bid
            bid_increase_pct = (bid_increase / prev_bid * 100) if prev_bid > 0 else 0
            
            prev_visits = db_data['prev_visits']
            visit_increase = visits - prev_visits
            
            # Prepara registro completo
            record = {
                "category": db_data["category"],
                "source": db_data["source"],
                "external_id": db_data["external_id"],
                "lot_number": db_data["lot_number"],
                "bid_actual": current_bid,
                "bid_has_bid": has_bid,
                "lot_visits": visits,
                "captured_at": datetime.now().isoformat(),
                # Metadados para análise (não vão para o banco)
                "_bid_increase": bid_increase,
                "_bid_increase_pct": bid_increase_pct,
                "_visit_increase": visit_increase,
            }
            
            matched_records.append(record)
            
            # Detecta "itens quentes" (critérios ajustáveis)
            is_hot = (
                bid_increase >= HOT_ITEM_THRESHOLD_VALUE or 
                bid_increase_pct >= HOT_ITEM_THRESHOLD_PERCENT
            )
            
            if is_hot:
                hot_items.append({
                    **record,
                    "lot_title": f"{api_data.get('lot_brand', '')} {api_data.get('lot_model', '')}".strip(),
                })
        
        print(f"✅ {len(matched_records)} matches encontrados\n")
        
        # Exibe itens quentes
        if hot_items:
            print(f"{'='*70}")
            print(f"🔥 {len(hot_items)} ITENS QUENTES DETECTADOS!")
            print(f"{'='*70}\n")
            
            # Ordena por aumento percentual
            hot_items.sort(key=lambda x: x['_bid_increase_pct'], reverse=True)
            
            for i, item in enumerate(hot_items[:10], 1):  # Top 10
                print(f"{i:2d}. 🚨 Lote {item['lot_number']}: {item['lot_title']}")
                print(f"      Lance: R$ {item['bid_actual']:,.2f} "
                      f"(+R$ {item['_bid_increase']:,.2f} / +{item['_bid_increase_pct']:.1f}%)")
                print(f"      Visitas: {item['lot_visits']} (+{item['_visit_increase']})\n")
        
        return matched_records, hot_items
    
    # ========================================================================
    # ETAPA 4: ATUALIZAR TABELAS BASE
    # ========================================================================
    
    def update_base_tables(self, records):
        """
        Atualiza tabelas base com dados de lances.
        
        ATUALIZA:
        - bid_actual (lance atual)
        - bid_has_bid (tem lance?)
        - lot_visits (visualizações)
        - last_scraped_at (timestamp)
        """
        if not records:
            return 0
        
        updated_count = 0
        
        # Agrupa por categoria (cada categoria é uma tabela)
        by_category = {}
        for record in records:
            cat = record["category"]
            if cat not in by_category:
                by_category[cat] = []
            by_category[cat].append(record)
        
        print("📝 Atualizando tabelas base...\n")
        
        for category, cat_records in by_category.items():
            cat_updated = 0
            cat_errors = 0
            
            for record in cat_records:
                try:
                    self.supabase.schema("auctions").table(category)\
                        .update({
                            "bid_actual": record["bid_actual"],
                            "bid_has_bid": record["bid_has_bid"],
                            "lot_visits": record["lot_visits"],
                            "last_scraped_at": record["captured_at"]
                        })\
                        .eq("source", record["source"])\
                        .eq("external_id", record["external_id"])\
                        .execute()
                    
                    cat_updated += 1
                    updated_count += 1
                    
                except Exception as e:
                    cat_errors += 1
                    continue
            
            # Log por categoria
            if cat_updated > 0:
                print(f"   ✅ {category:45s} | {cat_updated:3d} atualizados | {cat_errors:2d} erros")
            elif cat_errors > 0:
                print(f"   ❌ {category:45s} |   0 atualizados | {cat_errors:2d} erros")
        
        print()
        return updated_count
    
    # ========================================================================
    # ETAPA 5: SALVAR HISTÓRICO
    # ========================================================================
    
    def save_bid_history(self, records):
        """
        Salva histórico de lances na tabela auction_bid_history.
        
        ESTRATÉGIA:
        - Remove metadados de análise (_bid_increase, etc)
        - Remove duplicatas (mesmo lot no mesmo segundo)
        - Upsert com conflict resolution
        """
        if not records:
            return 0
        
        try:
            # Remove metadados internos
            clean_records = []
            for record in records:
                clean = {k: v for k, v in record.items() 
                        if not k.startswith('_')}
                clean_records.append(clean)
            
            # Remove duplicatas baseado em chave única
            unique_records = {}
            for record in clean_records:
                key = (
                    record["category"],
                    record["source"],
                    record["external_id"],
                    record["captured_at"][:19]  # Trunca para segundos
                )
                unique_records[key] = record
            
            records_to_insert = list(unique_records.values())
            
            # Upsert (insere ou atualiza se já existir)
            response = self.supabase.schema("auctions").table("auction_bid_history")\
                .upsert(records_to_insert, on_conflict="category,source,external_id,captured_at")\
                .execute()
            
            print(f"💾 {len(response.data)} registros salvos no histórico\n")
            return len(response.data)
            
        except Exception as e:
            print(f"❌ Erro ao salvar histórico: {e}\n")
            return 0
    
    # ========================================================================
    # ETAPA 6: EXECUTAR MONITORAMENTO COMPLETO
    # ========================================================================
    
    async def run(self):
        """Executa monitoramento completo (orquestração)"""
        print("\n" + "="*70)
        print("🔵 SODRÉ SANTORO MONITOR - DETECÇÃO DE LANCES")
        print("="*70)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        print()
        
        # 1. Carrega itens do banco
        if not self.load_database_items():
            return False
        
        if not self.db_items:
            print("⚠️ Nenhum item ativo no banco\n")
            return True
        
        # 2. Intercepta dados da API
        if not await self.intercept_sodre_data():
            print("❌ Falha ao capturar dados da API\n")
            return False
        
        # 3. Cruza dados
        matched_records, hot_items = self.cross_reference_data()
        
        if not matched_records:
            print("⚠️ Nenhum match encontrado entre DB e API\n")
            return True
        
        # 4. Atualiza tabelas base
        updated = self.update_base_tables(matched_records)
        
        # 5. Salva histórico
        saved = self.save_bid_history(matched_records)
        
        # 6. Resumo final
        print("="*70)
        print("📊 RESUMO DA EXECUÇÃO")
        print("="*70)
        print(f"📋 Itens no banco:        {len(self.db_items)}")
        print(f"🔵 Lotes da API:          {len(self.api_lots)}")
        print(f"🔗 Matches:               {len(matched_records)}")
        print(f"📝 Tabelas atualizadas:   {updated}")
        print(f"💾 Histórico salvo:       {saved}")
        print(f"🔥 Itens quentes:         {len(hot_items)}")
        print("="*70)
        
        match_rate = (len(matched_records) / len(self.db_items) * 100) if self.db_items else 0
        print(f"\n📈 Taxa de match: {match_rate:.1f}%")
        
        if match_rate < 50:
            print("\n⚠️ Taxa de match baixa! Possíveis causas:")
            print("   • Links no banco podem estar em formato diferente")
            print("   • Muitos lotes já finalizaram (não aparecem na API)")
            print("   • Paginação não capturou todas as páginas")
        
        print()
        return True


# ============================================================================
# EXECUÇÃO PRINCIPAL
# ============================================================================

async def main():
    """Ponto de entrada do script"""
    try:
        monitor = SodreMonitor()
        success = await monitor.run()
        
        if success:
            print("✅ Monitor executado com sucesso!\n")
            sys.exit(0)
        else:
            print("❌ Monitor falhou\n")
            sys.exit(1)
    
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sodré Santoro Monitor - Histórico de Lances

FUNCIONAMENTO:
1. Carrega TODOS os itens Sodré Santoro ativos da view vw_auctions_unified
2. Scraping via Playwright nas 4 categorias (pega links dos lotes)
3. Para cada lote matched: ENTRA na página e extrai tabela #tabela_lances
4. Atualiza tabelas base + salva histórico na auction_bid_history
"""

import os
import sys
import re
from datetime import datetime
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
from supabase import create_client, Client

# Configuração
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Categorias Sodré Santoro
SODRE_CATEGORIES = [
    'https://www.sodresantoro.com.br/veiculos/lotes?sort=auction_date_init_asc',
    'https://www.sodresantoro.com.br/materiais/lotes?sort=auction_date_init_asc',
    'https://www.sodresantoro.com.br/imoveis/lotes?sort=auction_date_init_asc',
    'https://www.sodresantoro.com.br/sucatas/lotes?sort=auction_date_init_asc',
]


class SodreSantoroMonitor:
    """Monitor de lances Sodré Santoro"""
    
    def __init__(self):
        """Inicializa conexões"""
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("SUPABASE_URL e SUPABASE_KEY devem estar definidas")
        
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.db_items = {}  # {link_normalizado: {category, source, external_id, lot_number}}
    
    @staticmethod
    def normalize_link(link: str) -> str:
        """Remove UTM params e normaliza link"""
        if not link:
            return ""
        parsed = urlparse(link)
        # Remove query params
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')
    
    def load_database_items(self):
        """Carrega TODOS os itens ativos do banco indexados por link"""
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
                        normalized = self.normalize_link(link)
                        self.db_items[normalized] = {
                            "category": item.get("category"),
                            "source": item.get("source"),
                            "external_id": item.get("external_id"),
                            "lot_number": item.get("lot_number"),
                        }
                
                total_loaded += len(response.data)
                print(f"   → Carregados {total_loaded} itens...")
                
                if len(response.data) < page_size:
                    break
                
                offset += page_size
            
            print(f"✅ {len(self.db_items)} itens Sodré Santoro carregados da view")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao carregar itens: {e}")
            return False
    
    def scrape_lot_links(self, page, category_url: str):
        """Scraping de links de lotes de uma categoria"""
        lot_links = []
        category_name = category_url.split('/')[3]  # extrai 'veiculos', 'materiais', etc
        
        try:
            print(f"   → Acessando listagem {category_name}...")
            page.goto(category_url, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(3000)
            
            # Scroll para carregar mais lotes (scroll infinito)
            prev_count = 0
            no_change_count = 0
            
            for scroll_attempt in range(15):  # Máximo 15 scrolls
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(2000)
                
                # Conta lotes atuais
                current_lots = page.query_selector_all('a[href*="/lote/"]')
                current_count = len(current_lots)
                
                if current_count == prev_count:
                    no_change_count += 1
                    if no_change_count >= 3:  # 3 scrolls sem mudança = fim
                        break
                else:
                    no_change_count = 0
                
                prev_count = current_count
            
            # Extrai todos os links únicos
            link_elements = page.query_selector_all('a[href*="/lote/"]')
            
            seen_links = set()
            for elem in link_elements:
                href = elem.get_attribute('href')
                if href and href not in seen_links:
                    if href.startswith('/'):
                        href = f"https://www.sodresantoro.com.br{href}"
                    seen_links.add(href)
                    lot_links.append(href)
            
            print(f"   → Encontrados {len(lot_links)} lotes únicos em {category_name}")
            
        except Exception as e:
            print(f"❌ Erro em {category_url}: {e}")
        
        return lot_links, category_name
    
    def extract_bid_data_from_lot_page(self, page, lot_url: str):
        """Entra na página do lote e extrai dados da tabela #tabela_lances"""
        try:
            page.goto(lot_url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(2000)
            
            # Procura a tabela de lances
            table = page.query_selector('#tabela_lances')
            if not table:
                return None
            
            # Conta linhas de lances (tr com class contendo 'tr_')
            bid_rows = table.query_selector_all('tr[class*="tr_"]')
            total_bids = len(bid_rows)
            
            if total_bids == 0:
                return None
            
            # Pega o lance mais recente (primeiro da lista)
            first_row = bid_rows[0]
            tds = first_row.query_selector_all('td')
            
            if len(tds) < 2:
                return None
            
            # Segunda coluna = valor do lance
            value_text = tds[1].inner_text().strip()
            # Remove tudo exceto dígitos e vírgula
            value_clean = re.sub(r'[^\d,]', '', value_text)
            current_value = float(value_clean.replace(',', '.')) if value_clean else 0
            
            return {
                "total_bids": total_bids,
                "current_value": current_value,
            }
            
        except Exception as e:
            print(f"⚠️ Erro ao extrair {lot_url}: {e}")
            return None
    
    def process_lots(self, page, lot_links):
        """Processa lotes: match com DB + extrai dados"""
        matched_data = []
        
        for lot_url in lot_links:
            normalized_link = self.normalize_link(lot_url)
            
            # Verifica se o lote está no banco
            db_item = self.db_items.get(normalized_link)
            if not db_item:
                continue
            
            # Entra na página do lote e extrai dados
            bid_data = self.extract_bid_data_from_lot_page(page, lot_url)
            if not bid_data:
                continue
            
            matched_data.append({
                "link": lot_url,
                "category": db_item["category"],
                "source": db_item["source"],
                "external_id": db_item["external_id"],
                "lot_number": db_item["lot_number"],
                "total_bids": bid_data["total_bids"],
                "current_value": bid_data["current_value"],
            })
        
        return matched_data
    
    def create_history_records(self, matched_data):
        """Cria registros para histórico"""
        records = []
        captured_at = datetime.now().isoformat()
        
        for item in matched_data:
            records.append({
                "category": item["category"],
                "source": item["source"],
                "external_id": item["external_id"],
                "lot_number": item["lot_number"],
                "total_bids": item["total_bids"],
                "total_bidders": 0,  # Sodré não fornece
                "current_value": item["current_value"],
                "captured_at": captured_at,
            })
        
        return records
    
    def update_base_tables(self, records):
        """Atualiza tabelas base com dados de lances"""
        updated_count = 0
        
        for record in records:
            try:
                table_name = record["category"]
                
                self.supabase.schema("auctions").table(table_name)\
                    .update({
                        "total_bids": record["total_bids"],
                        "total_bidders": record["total_bidders"],
                        "value": record["current_value"],
                        "last_scraped_at": record["captured_at"]
                    })\
                    .eq("source", record["source"])\
                    .eq("external_id", record["external_id"])\
                    .execute()
                
                updated_count += 1
                
            except Exception as e:
                print(f"⚠️ Erro ao atualizar {record['category']}/{record['external_id']}: {e}")
                continue
        
        return updated_count
    
    def save_bid_history(self, records):
        """Salva histórico de lances em lote"""
        if not records:
            return 0
        
        try:
            unique_records = {}
            for record in records:
                key = (
                    record["category"],
                    record["source"],
                    record["external_id"],
                    record["captured_at"][:19]
                )
                unique_records[key] = record
            
            records_to_insert = list(unique_records.values())
            
            response = self.supabase.schema("auctions").table("auction_bid_history")\
                .upsert(records_to_insert, on_conflict="category,source,external_id,captured_at")\
                .execute()
            
            return len(response.data)
            
        except Exception as e:
            print(f"❌ Erro ao salvar histórico: {e}")
            return 0
    
    def run(self):
        """Executa monitoramento completo"""
        print("\n" + "="*70)
        print("🔵 SODRÉ SANTORO MONITOR - HISTÓRICO DE LANCES")
        print("="*70)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        if not self.load_database_items():
            print("❌ Falha ao carregar itens do banco")
            return False
        
        if not self.db_items:
            print("⚠️ Nenhum item ativo encontrado no banco")
            return True
        
        # Scraping com Playwright
        all_matched = []
        category_stats = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()
            
            print("\n🌐 Fase 1: Coletando links dos lotes...\n")
            
            all_lot_links = []
            
            for category_url in SODRE_CATEGORIES:
                lot_links, cat_name = self.scrape_lot_links(page, category_url)
                
                # Filtra apenas links que estão no banco
                matched_links = []
                for link in lot_links:
                    normalized = self.normalize_link(link)
                    if normalized in self.db_items:
                        matched_links.append(link)
                
                all_lot_links.extend(matched_links)
                
                category_stats.append({
                    "name": cat_name,
                    "total_links": len(lot_links),
                    "matched_links": len(matched_links)
                })
            
            # Exibe stats da fase 1
            for stat in category_stats:
                name = stat["name"]
                total = stat["total_links"]
                matched = stat["matched_links"]
                
                if matched > 0:
                    print(f"✅ {name:25s} | {total:3d} lotes | {matched:3d} no banco")
                else:
                    print(f"⚪ {name:25s} | {total:3d} lotes | 0 no banco")
            
            print(f"\n📊 Total de lotes matched: {len(all_lot_links)}")
            
            if not all_lot_links:
                browser.close()
                print("\n⚠️ Nenhum lote matched para processar")
                return True
            
            print(f"\n🔍 Fase 2: Entrando em cada lote para extrair lances...\n")
            
            # Processa lotes em lote (a cada 50 para dar feedback)
            batch_size = 50
            processed = 0
            
            for i in range(0, len(all_lot_links), batch_size):
                batch = all_lot_links[i:i+batch_size]
                batch_matched = self.process_lots(page, batch)
                all_matched.extend(batch_matched)
                
                processed += len(batch)
                print(f"   → Processados {processed}/{len(all_lot_links)} lotes | {len(batch_matched)} com lances")
            
            browser.close()
        
        # Cria registros de histórico
        all_records = self.create_history_records(all_matched)
        
        print("\n" + "="*70)
        print("🔄 Atualizando tabelas base (total_bids, value, last_scraped_at)...")
        
        updated = self.update_base_tables(all_records)
        
        print("\n💾 Salvando histórico de lances na tabela auction_bid_history...")
        
        saved = self.save_bid_history(all_records)
        
        print("\n" + "="*70)
        print("📊 RESUMO DA EXECUÇÃO")
        print("="*70)
        print(f"📋 Itens Sodré Santoro na view: {len(self.db_items)}")
        print(f"🔗 Lotes matched (no banco): {len(all_lot_links)}")
        print(f"🎯 Lotes com lances extraídos: {len(all_matched)}")
        print(f"🔄 Tabelas base atualizadas: {updated}")
        print(f"💾 Registros salvos no histórico: {saved}")
        print("="*70)
        
        if len(all_lot_links) > 0:
            print(f"\n📈 Taxa de extração: {(len(all_matched)/len(all_lot_links)*100):.1f}%")
        
        return True


def main():
    """Execução principal"""
    try:
        monitor = SodreSantoroMonitor()
        success = monitor.run()
        
        if success:
            print("\n✅ Monitor executado com sucesso!")
            sys.exit(0)
        else:
            print("\n❌ Monitor falhou")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Erro fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
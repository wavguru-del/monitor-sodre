#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sodré Santoro Monitor - Histórico de Lances (ASYNC)

FUNCIONAMENTO OTIMIZADO COM ASYNCIO:
1. Carrega TODOS os itens ativos da view
2. Processa em PARALELO (asyncio) para máxima velocidade
3. UPDATE tabelas base + INSERT histórico
"""

import os
import sys
import re
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright
from supabase import create_client, Client

# Configuração
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Paralelismo (ajuste conforme necessário)
MAX_CONCURRENT = 15  # 15 lotes simultâneos


class SodreSantoroMonitor:
    """Monitor de lances Sodré Santoro (ASYNC)"""
    
    def __init__(self):
        """Inicializa conexões"""
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("SUPABASE_URL e SUPABASE_KEY devem estar definidas")
        
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.db_items = []
        self.processed = 0
        self.with_bids = 0
    
    def load_database_items(self):
        """Carrega TODOS os itens ativos do banco"""
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
                    if item.get("link"):
                        self.db_items.append({
                            "link": item.get("link"),
                            "category": item.get("category"),
                            "source": item.get("source"),
                            "external_id": item.get("external_id"),
                            "lot_number": item.get("lot_number"),
                        })
                
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
    
    async def extract_bid_data_from_lot_page(self, page, lot_url: str):
        """Entra na página do lote e extrai dados da tabela #tabela_lances"""
        try:
            await page.goto(lot_url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(0.8)
            
            # Procura a tabela de lances
            table = await page.query_selector('#tabela_lances')
            if not table:
                return None
            
            # Conta linhas de lances
            bid_rows = await table.query_selector_all('tr[class*="tr_"]')
            total_bids = len(bid_rows)
            
            if total_bids == 0:
                return None
            
            # Pega o lance mais recente (primeiro da lista)
            first_row = bid_rows[0]
            tds = await first_row.query_selector_all('td')
            
            if len(tds) < 2:
                return None
            
            # Segunda coluna = valor do lance
            value_text = await tds[1].inner_text()
            value_text = value_text.strip()
            value_clean = re.sub(r'[^\d,]', '', value_text)
            current_value = float(value_clean.replace(',', '.')) if value_clean else 0
            
            return {
                "total_bids": total_bids,
                "current_value": current_value,
            }
            
        except Exception as e:
            return None
    
    async def process_single_lot(self, browser, item, semaphore):
        """Processa um único lote (com semaphore para limitar paralelismo)"""
        async with semaphore:
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                locale='pt-BR',
            )
            page = await context.new_page()
            
            try:
                lot_url = item["link"]
                bid_data = await self.extract_bid_data_from_lot_page(page, lot_url)
                
                self.processed += 1
                
                # Log a cada 50 processados
                if self.processed % 50 == 0:
                    print(f"   → Processados {self.processed}/{len(self.db_items)} lotes | {self.with_bids} com lances")
                
                if not bid_data:
                    return None
                
                self.with_bids += 1
                
                return {
                    "link": lot_url,
                    "category": item["category"],
                    "source": item["source"],
                    "external_id": item["external_id"],
                    "lot_number": item["lot_number"],
                    "total_bids": bid_data["total_bids"],
                    "current_value": bid_data["current_value"],
                }
                
            except Exception as e:
                return None
            finally:
                await context.close()
    
    async def process_all_lots_async(self):
        """Processa todos os lotes em paralelo com asyncio"""
        total = len(self.db_items)
        print(f"\n🔍 Processando {total} lotes em paralelo (max {MAX_CONCURRENT} simultâneos)...\n")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            
            # Semaphore para limitar concorrência
            semaphore = asyncio.Semaphore(MAX_CONCURRENT)
            
            # Cria tasks para todos os lotes
            tasks = [
                self.process_single_lot(browser, item, semaphore)
                for item in self.db_items
            ]
            
            # Executa em paralelo
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            await browser.close()
        
        # Filtra apenas resultados válidos
        matched_data = [r for r in results if r and not isinstance(r, Exception)]
        
        print(f"   ✅ Processamento concluído: {len(matched_data)}/{total} lotes com lances")
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
                "total_bidders": 0,
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
    
    async def run_async(self):
        """Executa monitoramento completo (async)"""
        print("\n" + "="*70)
        print("🔵 SODRÉ SANTORO MONITOR - HISTÓRICO DE LANCES (ASYNC)")
        print("="*70)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⚡ Paralelismo: {MAX_CONCURRENT} lotes simultâneos")
        print("="*70)
        
        if not self.load_database_items():
            print("❌ Falha ao carregar itens do banco")
            return False
        
        if not self.db_items:
            print("⚠️ Nenhum item ativo encontrado no banco")
            return True
        
        # Processa todos os lotes em paralelo
        all_matched = await self.process_all_lots_async()
        
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
        print(f"🎯 Lotes com lances extraídos: {len(all_matched)}")
        print(f"🔄 Tabelas base atualizadas: {updated}")
        print(f"💾 Registros salvos no histórico: {saved}")
        print("="*70)
        
        if len(self.db_items) > 0:
            print(f"\n📈 Taxa de extração: {(len(all_matched)/len(self.db_items)*100):.1f}%")
        
        return True


def main():
    """Execução principal"""
    try:
        monitor = SodreSantoroMonitor()
        success = asyncio.run(monitor.run_async())
        
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
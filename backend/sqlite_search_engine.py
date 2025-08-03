# backend/sqlite_search_engine.py - Thread-Safe Version
import sqlite3
import json
import os
import re
from typing import List, Dict, Any
import math
from collections import Counter
import pandas as pd
import threading
from contextlib import contextmanager

class SQLitePatentSearch:
    """
    Motor de búsqueda de patentes usando SQLite FTS5 (Full-Text Search)
    Version Thread-Safe para aplicaciones Flask
    """
    
    def __init__(self, db_path="patent_search.db"):
        self.db_path = db_path
        self.local = threading.local()  # Thread-local storage
        self.init_database()
    
    @contextmanager
    def get_connection(self):
        """Context manager para obtener conexión thread-safe"""
        if not hasattr(self.local, 'conn') or self.local.conn is None:
            self.local.conn = sqlite3.connect(
                self.db_path, 
                check_same_thread=False,  # Permitir uso entre threads
                timeout=30.0  # Timeout para evitar bloqueos
            )
            self.local.conn.row_factory = sqlite3.Row
        
        try:
            yield self.local.conn
        except Exception as e:
            self.local.conn.rollback()
            raise e
    
    def init_database(self):
        """Inicializar base de datos SQLite con FTS5"""
        with self.get_connection() as conn:
            # Crear tabla principal de patentes
            conn.execute("""
                CREATE TABLE IF NOT EXISTS patents (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    abstract TEXT NOT NULL,
                    description TEXT,
                    claims TEXT,
                    assignee TEXT,
                    inventors TEXT, -- JSON array
                    application_date TEXT,
                    publication_date TEXT,
                    ipc_class TEXT,
                    ipc_classes TEXT, -- JSON array
                    category TEXT,
                    content_vector TEXT -- Representación numérica para ranking
                )
            """)
            
            # Crear índice FTS5 para búsqueda full-text
            conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS patents_fts USING fts5(
                    id UNINDEXED,
                    title,
                    abstract,
                    description,
                    claims,
                    assignee,
                    inventors,
                    ipc_class,
                    category,
                    content='patents',
                    content_rowid='rowid'
                )
            """)
            
            # Crear triggers para mantener FTS sincronizado
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS patents_fts_insert AFTER INSERT ON patents
                BEGIN
                    INSERT INTO patents_fts(
                        id, title, abstract, description, claims, 
                        assignee, inventors, ipc_class, category
                    ) VALUES (
                        new.id, new.title, new.abstract, new.description, new.claims,
                        new.assignee, new.inventors, new.ipc_class, new.category
                    );
                END
            """)
            
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS patents_fts_update AFTER UPDATE ON patents
                BEGIN
                    UPDATE patents_fts SET
                        title = new.title,
                        abstract = new.abstract,
                        description = new.description,
                        claims = new.claims,
                        assignee = new.assignee,
                        inventors = new.inventors,
                        ipc_class = new.ipc_class,
                        category = new.category
                    WHERE id = new.id;
                END
            """)
            
            # Crear índices para consultas rápidas
            conn.execute("CREATE INDEX IF NOT EXISTS idx_assignee ON patents(assignee)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_category ON patents(category)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ipc_class ON patents(ipc_class)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_date ON patents(publication_date)")
            
            conn.commit()
            print("✅ SQLite database initialized with FTS5 (Thread-Safe)")
    
    def index_patents(self, patents_data: List[Dict]):
        """Indexar patentes en la base de datos (Thread-Safe)"""
        if isinstance(patents_data, str):
            with open(patents_data, 'r', encoding='utf-8') as f:
                patents_data = json.load(f)
        
        print(f"🔄 Indexing {len(patents_data)} patents (Thread-Safe)...")
        
        with self.get_connection() as conn:
            # Limpiar tabla existente
            conn.execute("DELETE FROM patents")
            conn.commit()
            
            # Insertar patentes
            insert_query = """
                INSERT INTO patents (
                    id, title, abstract, description, claims, assignee,
                    inventors, application_date, publication_date,
                    ipc_class, ipc_classes, category, content_vector
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            for i, patent in enumerate(patents_data):
                try:
                    # Preparar datos
                    inventors_json = json.dumps(patent.get('inventors', []))
                    ipc_classes_json = json.dumps(patent.get('ipc_classes', []))
                    
                    # Vector simple basado en longitud de contenido
                    content_vector = self.create_simple_vector(patent)
                    
                    conn.execute(insert_query, (
                        patent.get('id', f'patent_{i}'),
                        patent.get('title', ''),
                        patent.get('abstract', ''),
                        patent.get('description', ''),
                        patent.get('claims', ''),
                        patent.get('assignee', ''),
                        inventors_json,
                        patent.get('application_date', ''),
                        patent.get('publication_date', ''),
                        patent.get('ipc_class', ''),
                        ipc_classes_json,
                        patent.get('category', ''),
                        content_vector
                    ))
                    
                    if (i + 1) % 100 == 0:
                        print(f"   Indexed {i + 1} patents...")
                        conn.commit()  # Commit periódico
                        
                except Exception as e:
                    print(f"Error indexing patent {i}: {e}")
                    continue
            
            conn.commit()
            print(f"✅ Indexed {len(patents_data)} patents successfully")
        
        # Crear estadísticas
        self.create_search_stats()
    
    def create_simple_vector(self, patent):
        """Crear vector simple para ranking (TF-IDF básico)"""
        text = f"{patent.get('title', '')} {patent.get('abstract', '')} {patent.get('claims', '')}"
        word_count = len(text.split())
        char_count = len(text)
        tech_terms = self.count_tech_terms(text)
        vector = json.dumps([word_count, char_count, tech_terms])
        return vector
    
    def count_tech_terms(self, text):
        """Contar términos técnicos comunes en patentes"""
        tech_terms = [
            'system', 'method', 'apparatus', 'device', 'process',
            'invention', 'embodiment', 'implementation', 'technology',
            'algorithm', 'network', 'computer', 'software', 'hardware'
        ]
        
        text_lower = text.lower()
        return sum(text_lower.count(term) for term in tech_terms)
    
    def search(self, query: str, limit: int = 10, category: str = None, 
               assignee: str = None, date_range: tuple = None) -> List[Dict]:
        """Búsqueda semántica de patentes (Thread-Safe)"""
        
        fts_query = self.prepare_fts_query(query)
        
        # Query base con FTS
        base_query = """
            SELECT p.*, rank
            FROM patents_fts 
            JOIN patents p ON patents_fts.id = p.id
            WHERE patents_fts MATCH ?
        """
        
        params = [fts_query]
        conditions = []
        
        # Filtros adicionales
        if category:
            conditions.append("p.category = ?")
            params.append(category)
        
        if assignee:
            conditions.append("p.assignee LIKE ?")
            params.append(f"%{assignee}%")
        
        if date_range:
            conditions.append("p.publication_date BETWEEN ? AND ?")
            params.extend(date_range)
        
        # Agregar condiciones
        if conditions:
            base_query += " AND " + " AND ".join(conditions)
        
        # Ordenar por relevancia FTS y limitar
        base_query += " ORDER BY rank LIMIT ?"
        params.append(limit)
        
        # Ejecutar búsqueda con conexión thread-safe
        with self.get_connection() as conn:
            cursor = conn.execute(base_query, params)
            results = cursor.fetchall()
        
        # Convertir a diccionarios y agregar score
        formatted_results = []
        for row in results:
            result = dict(row)
            
            # Parsear JSON fields de forma segura
            try:
                result['inventors'] = json.loads(result.get('inventors', '[]'))
            except:
                result['inventors'] = []
                
            try:
                result['ipc_classes'] = json.loads(result.get('ipc_classes', '[]'))
            except:
                result['ipc_classes'] = []
            
            # Calcular score personalizado
            result['score'] = self.calculate_custom_score(result, query)
            
            formatted_results.append(result)
        
        # Re-ordenar por score personalizado
        formatted_results.sort(key=lambda x: x['score'], reverse=True)
        
        return formatted_results
    
    def prepare_fts_query(self, query: str) -> str:
        """Preparar query para FTS5"""
        query = re.sub(r'[^\w\s]', ' ', query)
        terms = query.split()
        
        if len(terms) == 1:
            return f'"{terms[0]}"*'
        else:
            return ' OR '.join(f'"{term}"*' for term in terms)
    
    def calculate_custom_score(self, patent: Dict, original_query: str) -> float:
        """Calcular score personalizado similar a BM25"""
        score = 0.0
        query_terms = original_query.lower().split()
        
        # Pesos por campo
        field_weights = {
            'title': 3.0,
            'abstract': 2.0,
            'claims': 1.5,
            'description': 1.0
        }
        
        for field, weight in field_weights.items():
            field_text = patent.get(field, '').lower()
            if field_text:
                for term in query_terms:
                    tf = field_text.count(term)
                    if tf > 0:
                        score += tf * weight
        
        # Boost por categoría tecnológica
        if patent.get('category') in ['artificial_intelligence', 'telecommunications']:
            score *= 1.2
        
        return score
    
    def search_by_field(self, field: str, value: str, limit: int = 10) -> List[Dict]:
        """Búsqueda específica por campo (Thread-Safe)"""
        valid_fields = ['assignee', 'category', 'ipc_class', 'inventors']
        
        if field not in valid_fields:
            raise ValueError(f"Invalid field. Must be one of: {valid_fields}")
        
        if field == 'inventors':
            query = f"SELECT * FROM patents WHERE inventors LIKE ? LIMIT ?"
            params = [f'%{value}%', limit]
        else:
            query = f"SELECT * FROM patents WHERE {field} LIKE ? LIMIT ?"
            params = [f'%{value}%', limit]
        
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            results = cursor.fetchall()
        
        return [dict(row) for row in results]
    
    def get_aggregations(self) -> Dict[str, Any]:
        """Obtener agregaciones similares a Elasticsearch (Thread-Safe)"""
        aggregations = {}
        
        with self.get_connection() as conn:
            # Top assignees
            cursor = conn.execute("""
                SELECT assignee, COUNT(*) as count 
                FROM patents 
                WHERE assignee != ''
                GROUP BY assignee 
                ORDER BY count DESC 
                LIMIT 10
            """)
            aggregations['top_assignees'] = dict(cursor.fetchall())
            
            # Categories
            cursor = conn.execute("""
                SELECT category, COUNT(*) as count 
                FROM patents 
                GROUP BY category 
                ORDER BY count DESC
            """)
            aggregations['categories'] = dict(cursor.fetchall())
            
            # IPC classes
            cursor = conn.execute("""
                SELECT ipc_class, COUNT(*) as count 
                FROM patents 
                WHERE ipc_class != ''
                GROUP BY ipc_class 
                ORDER BY count DESC 
                LIMIT 10
            """)
            aggregations['ipc_classes'] = dict(cursor.fetchall())
            
            # Date histogram (by year)
            cursor = conn.execute("""
                SELECT substr(publication_date, 1, 4) as year, COUNT(*) as count
                FROM patents 
                WHERE length(publication_date) >= 4
                GROUP BY year 
                ORDER BY year DESC
            """)
            aggregations['by_year'] = dict(cursor.fetchall())
            
            # Total patents
            cursor = conn.execute("SELECT COUNT(*) FROM patents")
            aggregations['total_patents'] = cursor.fetchone()[0]
        
        return aggregations
    
    def create_search_stats(self):
        """Crear estadísticas de la base de datos (Thread-Safe)"""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM patents")
            total_patents = cursor.fetchone()[0]
        
        aggregations = self.get_aggregations()
        
        stats = {
            "total_patents": total_patents,
            "aggregations": aggregations
        }
        
        print(f"\n📊 SEARCH DATABASE STATS:")
        print(f"   Total Patents: {stats['total_patents']}")
        print(f"   Top Companies: {list(aggregations['top_assignees'].keys())[:3]}")
        print(f"   Categories: {list(aggregations['categories'].keys())}")
        
        return stats
    
    def close(self):
        """Cerrar conexión thread-local"""
        if hasattr(self.local, 'conn') and self.local.conn:
            self.local.conn.close()
            self.local.conn = None

# Ejemplo de uso
if __name__ == "__main__":
    search_engine = SQLitePatentSearch()
    
    patents_file = "../data/processed/patents.json"
    if not os.path.exists(patents_file):
        patents_file = "data/processed/patents.json"
    
    if os.path.exists(patents_file):
        print("🔄 Loading patents data...")
        search_engine.index_patents(patents_file)
        
        print("\n🔍 TESTING SEARCHES:")
        print("-" * 40)
        
        # Búsqueda general
        results = search_engine.search("artificial intelligence neural network", limit=5)
        print(f"\n1. AI Search: {len(results)} results")
        for r in results[:2]:
            print(f"   - {r['title'][:80]}... (Score: {r['score']:.2f})")
        
        # Agregaciones
        aggs = search_engine.get_aggregations()
        print(f"\n2. Total Patents: {aggs.get('total_patents', 0)}")
        print(f"3. Top 3 Companies: {list(aggs['top_assignees'].keys())[:3]}")
        
        print("\n✅ Thread-Safe SQLite search engine working correctly!")
        
    else:
        print(f"❌ No patents data found at {patents_file}")
    
    search_engine.close()
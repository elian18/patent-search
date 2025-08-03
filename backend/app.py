# backend/app.py - Thread-Safe Version
from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import json
import logging
from datetime import datetime
import traceback

# Intentar importar ambos motores de búsqueda
try:
    from elasticsearch import Elasticsearch
    ELASTICSEARCH_AVAILABLE = True
except ImportError:
    ELASTICSEARCH_AVAILABLE = False

try:
    from sqlite_search_engine import SQLitePatentSearch
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

app = Flask(__name__)
CORS(app)

# Configurar logging más detallado
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PatentSearchAPI:
    def __init__(self):
        self.search_engine = None
        self.engine_type = None
        self.setup_search_engine()
    
    def setup_search_engine(self):
        """Configurar motor de búsqueda disponible"""
        
        # Intentar Elasticsearch primera
        if ELASTICSEARCH_AVAILABLE:
            try:
                es_client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
                if es_client.ping():
                    self.search_engine = es_client
                    self.engine_type = "elasticsearch"
                    logger.info("✅ Using Elasticsearch search engine")
                    return
            except Exception as e:
                logger.warning(f"Elasticsearch not available: {e}")
        
        # Fallback a SQLite (Thread-Safe)
        if SQLITE_AVAILABLE:
            try:
                self.search_engine = SQLitePatentSearch()
                self.engine_type = "sqlite"
                logger.info("✅ Using SQLite FTS search engine (Thread-Safe)")
                return
            except Exception as e:
                logger.error(f"SQLite engine failed: {e}")
                logger.error(traceback.format_exc())
        
        # Sin motor de búsqueda
        logger.error("❌ No search engine available!")
        self.search_engine = None
        self.engine_type = None
    
    def search_patents(self, query, limit=10, filters=None):
        """Búsqueda unificada independiente del motor"""
        if not self.search_engine:
            return {"error": "No search engine available", "results": []}
        
        try:
            if self.engine_type == "elasticsearch":
                return self.search_elasticsearch(query, limit, filters)
            elif self.engine_type == "sqlite":
                return self.search_sqlite(query, limit, filters)
        except Exception as e:
            logger.error(f"Search error: {e}")
            logger.error(traceback.format_exc())
            return {"error": str(e), "results": []}
    
    def search_elasticsearch(self, query, limit, filters):
        """Búsqueda usando Elasticsearch"""
        search_body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "abstract^2", "claims^1.5", "description"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            },
            "highlight": {
                "fields": {
                    "title": {},
                    "abstract": {}
                }
            },
            "size": limit
        }
        
        # Aplicar filtros
        if filters:
            bool_query = {"must": [search_body["query"]]}
            
            if filters.get("category"):
                bool_query["filter"] = [{"term": {"category": filters["category"]}}]
            
            if filters.get("assignee"):
                bool_query["filter"] = bool_query.get("filter", [])
                bool_query["filter"].append({"wildcard": {"assignee": f"*{filters['assignee']}*"}})
            
            search_body["query"] = {"bool": bool_query}
        
        response = self.search_engine.search(index="patents", body=search_body)
        
        results = []
        for hit in response["hits"]["hits"]:
            result = hit["_source"]
            result["score"] = hit["_score"]
            result["highlights"] = hit.get("highlight", {})
            results.append(result)
        
        return {
            "total": response["hits"]["total"]["value"],
            "results": results,
            "engine": "elasticsearch"
        }
    
    def search_sqlite(self, query, limit, filters):
        """Búsqueda usando SQLite (Thread-Safe)"""
        try:
            category = filters.get("category") if filters else None
            assignee = filters.get("assignee") if filters else None
            
            results = self.search_engine.search(
                query=query,
                limit=limit,
                category=category,
                assignee=assignee
            )
            
            return {
                "total": len(results),
                "results": results,
                "engine": "sqlite"
            }
        except Exception as e:
            logger.error(f"SQLite search error: {e}")
            logger.error(traceback.format_exc())
            raise e
    
    def get_aggregations(self):
        """Obtener agregaciones/estadísticas"""
        if not self.search_engine:
            return {}
        
        try:
            if self.engine_type == "elasticsearch":
                # Elasticsearch aggregations
                agg_body = {
                    "size": 0,
                    "aggs": {
                        "categories": {"terms": {"field": "category", "size": 10}},
                        "assignees": {"terms": {"field": "assignee.keyword", "size": 10}},
                        "ipc_classes": {"terms": {"field": "ipc_class", "size": 10}}
                    }
                }
                
                response = self.search_engine.search(index="patents", body=agg_body)
                return response["aggregations"]
                
            elif self.engine_type == "sqlite":
                return self.search_engine.get_aggregations()
                
        except Exception as e:
            logger.error(f"Aggregation error: {e}")
            logger.error(traceback.format_exc())
            return {}
    
    def load_data(self, patents_file=None):
        """Cargar datos en el motor de búsqueda"""
        if not patents_file:
            # Buscar archivo de patentes
            possible_paths = [
                "../data/processed/patents.json",
                "data/processed/patents.json",
                "patents.json"
            ]
            
            for path in possible_paths:
                if os.path.exists(path):
                    patents_file = path
                    break
        
        if not patents_file or not os.path.exists(patents_file):
            logger.error(f"Patents data file not found. Tried: {possible_paths}")
            return {"error": "Patents data file not found", "success": False}
        
        try:
            logger.info(f"Loading patents from: {patents_file}")
            
            if self.engine_type == "elasticsearch":
                return self.load_data_elasticsearch(patents_file)
            elif self.engine_type == "sqlite":
                return self.load_data_sqlite(patents_file)
                
        except Exception as e:
            logger.error(f"Data loading error: {e}")
            logger.error(traceback.format_exc())
            return {"error": str(e), "success": False}
    
    def load_data_elasticsearch(self, patents_file):
        """Cargar datos en Elasticsearch"""
        with open(patents_file, 'r', encoding='utf-8') as f:
            patents = json.load(f)
        
        # Crear índice con mapping
        mapping = {
            "mappings": {
                "properties": {
                    "title": {"type": "text", "analyzer": "standard"},
                    "abstract": {"type": "text", "analyzer": "standard"},
                    "description": {"type": "text", "analyzer": "standard"},
                    "claims": {"type": "text", "analyzer": "standard"},
                    "assignee": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "category": {"type": "keyword"},
                    "ipc_class": {"type": "keyword"},
                    "application_date": {"type": "date"},
                    "publication_date": {"type": "date"}
                }
            }
        }
        
        # Recrear índice
        if self.search_engine.indices.exists(index="patents"):
            self.search_engine.indices.delete(index="patents")
        
        self.search_engine.indices.create(index="patents", body=mapping)
        
        # Indexar documentos
        for i, patent in enumerate(patents):
            self.search_engine.index(
                index="patents",
                id=patent.get("id", f"patent_{i}"),
                body=patent
            )
        
        return {"success": True, "indexed": len(patents)}
    
    def load_data_sqlite(self, patents_file):
        """Cargar datos en SQLite (Thread-Safe)"""
        try:
            # Verificar que el archivo existe y es válido
            with open(patents_file, 'r', encoding='utf-8') as f:
                patents = json.load(f)
            
            logger.info(f"Found {len(patents)} patents in file")
            
            # Indexar usando el método thread-safe
            self.search_engine.index_patents(patents)
            
            return {
                "success": True, 
                "message": f"Data loaded into SQLite ({len(patents)} patents)",
                "indexed": len(patents)
            }
            
        except Exception as e:
            logger.error(f"SQLite data loading error: {e}")
            logger.error(traceback.format_exc())
            raise e

# Inicializar API
patent_api = PatentSearchAPI()

# RUTAS DE LA API
@app.route("/", methods=["GET"])
def home():
    """Página de inicio"""
    engine_status = "✅ Available" if patent_api.search_engine else "❌ Not Available"
    
    return jsonify({
        "message": "Patent Search API - Thread-Safe",
        "version": "1.1",
        "search_engine": patent_api.engine_type,
        "status": engine_status,
        "endpoints": {
            "search": "/api/search?q=<query>&limit=<number>",
            "setup": "/api/setup",
            "stats": "/api/stats",
            "search_by_field": "/api/search/<field>/<value>",
            "health": "/api/health"
        }
    })

@app.route("/api/search", methods=["GET"])
def search_patents():
    """Endpoint principal de búsqueda"""
    try:
        query = request.args.get("q", "")
        limit = int(request.args.get("limit", 10))
        
        if not query:
            return jsonify({"error": "Query parameter 'q' is required"}), 400
        
        # Filtros opcionales
        filters = {}
        if request.args.get("category"):
            filters["category"] = request.args.get("category")
        if request.args.get("assignee"):
            filters["assignee"] = request.args.get("assignee")
        
        logger.info(f"Search request: query='{query}', limit={limit}, filters={filters}")
        
        results = patent_api.search_patents(query, limit, filters)
        
        return jsonify({
            "query": query,
            "timestamp": datetime.now().isoformat(),
            **results
        })
        
    except Exception as e:
        logger.error(f"Search endpoint error: {e}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route("/api/search/<field>/<value>", methods=["GET"])
def search_by_field(field, value):
    """Búsqueda específica por campo"""
    try:
        limit = int(request.args.get("limit", 10))
        
        if patent_api.engine_type == "sqlite":
            try:
                results = patent_api.search_engine.search_by_field(field, value, limit)
                return jsonify({
                    "field": field,
                    "value": value,
                    "results": results,
                    "total": len(results)
                })
            except ValueError as e:
                return jsonify({"error": str(e)}), 400
        else:
            # Para Elasticsearch, usar búsqueda regular con filtro
            filters = {field: value}
            results = patent_api.search_patents("*", limit, filters)
            return jsonify(results)
            
    except Exception as e:
        logger.error(f"Search by field error: {e}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route("/api/stats", methods=["GET"])
def get_statistics():
    """Obtener estadísticas y agregaciones"""
    try:
        aggregations = patent_api.get_aggregations()
        
        return jsonify({
            "search_engine": patent_api.engine_type,
            "timestamp": datetime.now().isoformat(),
            "aggregations": aggregations
        })
        
    except Exception as e:
        logger.error(f"Stats endpoint error: {e}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route("/api/setup", methods=["POST"])
def setup_data():
    """Cargar datos en el motor de búsqueda"""
    try:
        patents_file = None
        
        if request.is_json and request.json:
            patents_file = request.json.get("file_path")
        
        logger.info(f"Setup request: file_path={patents_file}")
        
        result = patent_api.load_data(patents_file)
        
        status_code = 200 if result.get("success") else 500
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Setup endpoint error: {e}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e), "success": False}), 500

@app.route("/api/health", methods=["GET"])
def health_check():
    """Check de salud del sistema"""
    try:
        # Verificar si hay datos cargados
        has_data = False
        total_patents = 0
        
        if patent_api.search_engine and patent_api.engine_type == "sqlite":
            try:
                aggs = patent_api.get_aggregations()
                total_patents = aggs.get('total_patents', 0)
                has_data = total_patents > 0
            except:
                has_data = False
        
        return jsonify({
            "status": "healthy" if patent_api.search_engine else "degraded",
            "search_engine": patent_api.engine_type,
            "elasticsearch_available": ELASTICSEARCH_AVAILABLE,
            "sqlite_available": SQLITE_AVAILABLE,
            "has_data": has_data,
            "total_patents": total_patents,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return jsonify({
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    print("\n🚀 PATENT SEARCH API - THREAD-SAFE")
    print("=" * 50)
    print(f"Search Engine: {patent_api.engine_type or 'None'}")
    print(f"Elasticsearch: {'✅' if ELASTICSEARCH_AVAILABLE else '❌'}")
    print(f"SQLite FTS: {'✅' if SQLITE_AVAILABLE else '❌'}")
    
    # Verificar si hay datos
    if patent_api.search_engine and patent_api.engine_type == "sqlite":
        try:
            aggs = patent_api.get_aggregations()
            total = aggs.get('total_patents', 0)
            if total > 0:
                print(f"📊 Patents loaded: {total}")
            else:
                print("⚠️  No patents loaded - use /api/setup")
        except:
            print("⚠️  Database not initialized - use /api/setup")
    
    print("\n📖 API Endpoints:")
    print("   GET  /                    - API info")
    print("   GET  /api/search?q=<query> - Search patents")
    print("   GET  /api/stats           - Get statistics")
    print("   POST /api/setup           - Load patent data")
    print("   GET  /api/health          - Health check")
    print("\n🔍 Example searches:")
    print("   /api/search?q=artificial intelligence")
    print("   /api/search?q=neural network&category=physics")
    print("   /api/search?q=machine learning&assignee=Google")
    print("\n" + "=" * 50)
    
    # Ejecutar servidor con threading habilitado
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
# backend/parser.py
import xml.etree.ElementTree as ET
import json
import os
import re
from datetime import datetime
import pandas as pd

class USPTOParser:
    def __init__(self):
        self.processed_patents = []
        self.namespaces = {
            'us': 'http://www.uspto.gov',
            'default': ''
        }
        
    def clean_text(self, text):
        """Limpiar texto extraído del XML"""
        if not text:
            return ""
        
        # Remover caracteres especiales y normalizar espacios
        text = re.sub(r'\s+', ' ', text.strip())
        text = re.sub(r'[^\w\s\-\.,;:()\[\]]', ' ', text)
        return text[:5000]  # Limitar longitud para eficiencia
    
    def extract_text_content(self, element):
        """Extraer todo el texto de un elemento XML recursivamente"""
        if element is None:
            return ""
        
        texts = []
        if element.text:
            texts.append(element.text.strip())
        
        for child in element:
            child_text = self.extract_text_content(child)
            if child_text:
                texts.append(child_text)
            if child.tail:
                texts.append(child.tail.strip())
        
        return ' '.join(filter(None, texts))
    
    def find_element_flexible(self, root, *possible_paths):
        """Buscar elemento usando múltiples paths posibles"""
        for path in possible_paths:
            element = root.find(path)
            if element is not None:
                return element
        return None
    
    def parse_uspto_xml(self, xml_file_path):
        """Parsear archivo XML de USPTO - maneja múltiples documentos concatenados"""
        print(f"Parsing: {xml_file_path}")
        
        # Primero intentar como archivo XML único
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            return self.extract_from_root(root, xml_file_path)
        except ET.ParseError as e:
            print(f"Single XML parse failed: {e}")
            print("Trying to split multiple concatenated XML documents...")
            return self.parse_concatenated_xml(xml_file_path)
    
    def parse_concatenated_xml(self, xml_file_path):
        """Parsear archivo con múltiples documentos XML concatenados - FIXED"""
        patents = []
        
        try:
            with open(xml_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # FIXED: Mejor estrategia para separar documentos XML
            # Buscar patrones de inicio completos de documento
            documents = []
            
            # Patrón más robusto para encontrar documentos completos
            pattern = r'<\?xml version="1\.0"[^>]*\?>\s*<!DOCTYPE[^>]*>\s*<us-patent-application[^>]*>.*?</us-patent-application>'
            
            # Usar re.DOTALL para que . coincida con newlines
            matches = re.finditer(pattern, content, re.DOTALL)
            
            for match in matches:
                documents.append(match.group(0))
            
            print(f"Found {len(documents)} complete XML documents using regex")
            
            # Si regex no funciona, usar método de líneas mejorado
            if not documents:
                documents = self.split_by_lines_improved(content)
            
            # Procesar cada documento individual
            for i, doc_content in enumerate(documents[:50]):  # Limitar a 50 docs para prueba
                try:
                    # FIXED: Limpiar el documento antes de parsear
                    doc_content = doc_content.strip()
                    if not doc_content:
                        continue
                    
                    root = ET.fromstring(doc_content)
                    doc_patents = self.extract_from_root(root, f"{xml_file_path}_doc_{i}")
                    patents.extend(doc_patents)
                    
                    if (i + 1) % 10 == 0:
                        print(f"  Processed {i + 1} documents, {len(patents)} patents total...")
                        
                except ET.ParseError as e:
                    print(f"  Error parsing document {i+1}: {e}")
                    continue
                except Exception as e:
                    print(f"  Error processing document {i+1}: {e}")
                    continue
            
            print(f"Successfully extracted {len(patents)} patents from concatenated XML")
            return patents
            
        except Exception as e:
            print(f"Error reading file {xml_file_path}: {e}")
            return []
    
    def split_by_lines_improved(self, content):
        """FIXED: Método mejorado para separar documentos por líneas"""
        documents = []
        current_doc = ""
        doc_started = False
        doc_depth = 0
        
        lines = content.split('\n')
        
        for line in lines:
            line_stripped = line.strip()
            
            # Detectar inicio de nuevo documento
            if line_stripped.startswith('<?xml version="1.0"'):
                # Si ya teníamos un documento, guardarlo
                if doc_started and current_doc.strip():
                    documents.append(current_doc.strip())
                
                # Iniciar nuevo documento
                current_doc = line + '\n'
                doc_started = True
                doc_depth = 0
                
            elif doc_started:
                current_doc += line + '\n'
                
                # Contar depth de tags para saber cuándo termina el documento
                if '<us-patent-application' in line_stripped:
                    doc_depth += 1
                elif '</us-patent-application>' in line_stripped:
                    doc_depth -= 1
                    
                    # Si cerramos todos los us-patent-application, termina el documento
                    if doc_depth == 0:
                        documents.append(current_doc.strip())
                        current_doc = ""
                        doc_started = False
        
        # Agregar último documento si existe
        if current_doc.strip():
            documents.append(current_doc.strip())
        
        print(f"Found {len(documents)} XML documents using line-by-line method")
        return documents
    
    def extract_from_root(self, root, source_info):
        """Extraer patentes de un elemento raíz XML"""
        patents = []
        
        # Buscar todas las aplicaciones de patente
        patent_applications = []
        
        # Intentar diferentes estructuras comunes
        patent_applications.extend(root.findall('.//us-patent-application'))
        patent_applications.extend(root.findall('.//patent-application-publication'))
        patent_applications.extend(root.findall('.//us-patent-grant'))
        
        # Si el root mismo es una patente
        if not patent_applications:
            if (root.tag.endswith('patent-application') or 
                root.tag.endswith('patent-grant') or 
                'patent' in root.tag.lower()):
                patent_applications = [root]
        
        print(f"  Found {len(patent_applications)} patent applications in {source_info}")
        
        for i, patent_app in enumerate(patent_applications):
            try:
                patent_data = self.extract_patent_data_robust(patent_app)
                if patent_data and patent_data.get('title') and patent_data.get('abstract'):
                    patents.append(patent_data)
            except Exception as e:
                print(f"    Error processing patent {i+1}: {e}")
                continue
        
        return patents
    
    def extract_patent_data_robust(self, patent_elem):
        """Extraer datos de patente con múltiples estrategias de búsqueda"""
        patent_data = {}
        
        # 1. ID de Patente - múltiples ubicaciones posibles
        patent_data['id'] = self.extract_patent_id(patent_elem)
        
        # 2. Título
        title_paths = [
            './/invention-title',
            './/title-of-invention', 
            './/invention-title-text',
            './/title'
        ]
        title_elem = self.find_element_flexible(patent_elem, *title_paths)
        patent_data['title'] = self.clean_text(self.extract_text_content(title_elem)) if title_elem is not None else "Untitled"
        
        # 3. Abstract
        abstract_paths = [
            './/abstract',
            './/abstract-text',
            './/subdoc-abstract'
        ]
        abstract_elem = self.find_element_flexible(patent_elem, *abstract_paths)
        patent_data['abstract'] = self.clean_text(self.extract_text_content(abstract_elem)) if abstract_elem is not None else "No abstract available"
        
        # 4. Assignee (Empresa)
        patent_data['assignee'] = self.extract_assignee_robust(patent_elem)
        
        # 5. Inventores
        patent_data['inventors'] = self.extract_inventors_robust(patent_elem)
        
        # 6. Fechas
        patent_data.update(self.extract_dates_robust(patent_elem))
        
        # 7. Clasificaciones
        patent_data['ipc_classes'] = self.extract_classifications_robust(patent_elem)
        patent_data['ipc_class'] = patent_data['ipc_classes'][0] if patent_data['ipc_classes'] else 'G06F'
        
        # 8. Claims
        claims_paths = [
            './/claims',
            './/claim',
            './/subdoc-claims'
        ]
        claims_elem = self.find_element_flexible(patent_elem, *claims_paths)
        patent_data['claims'] = self.clean_text(self.extract_text_content(claims_elem)) if claims_elem is not None else "No claims available"
        
        # 9. Descripción
        description_paths = [
            './/description',
            './/detailed-description',
            './/subdoc-description'
        ]
        description_elem = self.find_element_flexible(patent_elem, *description_paths)
        description_text = self.extract_text_content(description_elem) if description_elem is not None else ""
        patent_data['description'] = self.clean_text(description_text)[:3000]  # Limitar para eficiencia
        
        # 10. Categorización automática
        patent_data['category'] = self.categorize_patent(patent_data['ipc_class'], patent_data['title'])
        
        # Validar datos mínimos
        if len(patent_data['title']) < 5 or len(patent_data['abstract']) < 10:
            return None
            
        return patent_data
    
    def extract_patent_id(self, patent_elem):
        """Extraer ID de patente con múltiples estrategias"""
        id_paths = [
            './/document-id/doc-number',
            './/publication-reference//doc-number',
            './/application-reference//doc-number',
            './/subdoc-bibliographic-information//document-id/doc-number',
            './/patent-number',
            './/application-number'
        ]
        
        for path in id_paths:
            id_elem = patent_elem.find(path)
            if id_elem is not None and id_elem.text:
                patent_id = id_elem.text.strip()
                if patent_id:
                    return patent_id
        
        # Generar ID único si no se encuentra
        return f"PATENT_{hash(str(patent_elem))}"[:15]
    
    def extract_assignee_robust(self, patent_elem):
        """Extraer assignee con múltiples estrategias"""
        assignee_paths = [
            './/assignees/assignee',
            './/assignee',
            './/subdoc-bibliographic-information//assignee'
        ]
        
        for path in assignee_paths:
            assignee_elem = patent_elem.find(path)
            if assignee_elem is not None:
                # Buscar nombre de organización
                org_paths = [
                    './/orgname',
                    './/organization-name',
                    './/assignee-name'
                ]
                
                for org_path in org_paths:
                    org_elem = assignee_elem.find(org_path)
                    if org_elem is not None and org_elem.text:
                        return org_elem.text.strip()
                
                # Si no es organización, buscar nombre de persona
                first_name = assignee_elem.find('.//first-name')
                last_name = assignee_elem.find('.//last-name')
                
                if first_name is not None and last_name is not None:
                    return f"{first_name.text} {last_name.text}".strip()
        
        return "Unknown Assignee"
    
    def extract_inventors_robust(self, patent_elem):
        """Extraer inventores con múltiples estrategias"""
        inventors = []
        
        inventor_paths = [
            './/inventors/inventor',
            './/inventor',
            './/subdoc-bibliographic-information//inventor'
        ]
        
        for path in inventor_paths:
            for inventor_elem in patent_elem.findall(path):
                first_name_elem = inventor_elem.find('.//first-name')
                last_name_elem = inventor_elem.find('.//last-name')
                
                if first_name_elem is not None and last_name_elem is not None:
                    name = f"{first_name_elem.text} {last_name_elem.text}".strip()
                    if name not in inventors:
                        inventors.append(name)
        
        return inventors if inventors else ["Unknown Inventor"]
    
    def extract_dates_robust(self, patent_elem):
        """Extraer fechas con múltiples estrategias"""
        dates = {}
        
        # Fecha de aplicación
        app_date_paths = [
            './/application-reference//date',
            './/filing-date',
            './/subdoc-bibliographic-information//filing-date'
        ]
        
        for path in app_date_paths:
            date_elem = patent_elem.find(path)
            if date_elem is not None and date_elem.text:
                dates['application_date'] = self.format_date(date_elem.text)
                break
        
        # Fecha de publicación
        pub_date_paths = [
            './/publication-reference//date',
            './/publication-date',
            './/subdoc-bibliographic-information//publication-date'
        ]
        
        for path in pub_date_paths:
            date_elem = patent_elem.find(path)
            if date_elem is not None and date_elem.text:
                dates['publication_date'] = self.format_date(date_elem.text)
                break
        
        # Defaults
        current_year = datetime.now().year
        if 'application_date' not in dates:
            dates['application_date'] = f"{current_year}-01-01"
        if 'publication_date' not in dates:
            dates['publication_date'] = f"{current_year}-01-01"
        
        return dates
    
    def format_date(self, date_str):
        """Formatear fecha USPTO a formato ISO"""
        try:
            # USPTO típicamente usa YYYYMMDD
            date_str = re.sub(r'[^\d]', '', date_str)
            
            if len(date_str) >= 8:
                year = date_str[:4]
                month = date_str[4:6]
                day = date_str[6:8]
                return f"{year}-{month}-{day}"
            elif len(date_str) >= 6:
                year = date_str[:4]
                month = date_str[4:6]
                return f"{year}-{month}-01"
            elif len(date_str) >= 4:
                year = date_str[:4]
                return f"{year}-01-01"
        except:
            pass
        
        return f"{datetime.now().year}-01-01"
    
    def extract_classifications_robust(self, patent_elem):
        """Extraer clasificaciones IPC/US"""
        classifications = []
        
        # Clasificaciones IPC
        ipc_paths = [
            './/classifications-ipc//main-classification',
            './/classification-ipc//main-classification',
            './/ipc-classification',
            './/classification-ipc'
        ]
        
        for path in ipc_paths:
            for ipc_elem in patent_elem.findall(path):
                if ipc_elem.text:
                    classifications.append(ipc_elem.text.strip())
        
        # Clasificaciones US si no hay IPC
        if not classifications:
            us_paths = [
                './/classification-us//main-classification',
                './/us-classification',
                './/classification-national'
            ]
            
            for path in us_paths:
                for us_elem in patent_elem.findall(path):
                    if us_elem.text:
                        classifications.append(f"US{us_elem.text.strip()}")
        
        return classifications if classifications else ["G06F"]
    
    def categorize_patent(self, ipc_class, title=""):
        """Categorización automática basada en IPC y título"""
        if not ipc_class:
            ipc_class = "G06F"
        
        # Categorías basadas en IPC
        ipc_categories = {
            'A': 'human_necessities',
            'B': 'performing_operations',
            'C': 'chemistry_metallurgy',
            'D': 'textiles_paper',
            'E': 'fixed_constructions',
            'F': 'mechanical_engineering',
            'G': 'physics',
            'H': 'electricity'
        }
        
        # Subcategorías más específicas
        specific_patterns = {
            'artificial_intelligence': ['G06N', 'AI', 'machine learning', 'neural network'],
            'telecommunications': ['H04', 'communication', 'wireless', 'network'],
            'biotechnology': ['C12', 'A61', 'genetic', 'biological', 'medical'],
            'semiconductors': ['H01L', 'semiconductor', 'transistor', 'chip'],
            'automotive': ['B60', 'vehicle', 'automotive', 'car'],
            'energy': ['H02', 'F03', 'solar', 'battery', 'energy'],
        }
        
        title_lower = title.lower()
        
        # Buscar categorías específicas primero
        for category, patterns in specific_patterns.items():
            for pattern in patterns:
                if pattern.lower() in title_lower or pattern in ipc_class:
                    return category
        
        # Fallback a categoría IPC general
        first_char = ipc_class[0].upper()
        return ipc_categories.get(first_char, 'other')
    
    def process_file(self, file_path, max_patents=None):
        """Procesar un archivo XML individual"""
        patents = self.parse_uspto_xml(file_path)
        
        if max_patents:
            patents = patents[:max_patents]
        
        return patents
    
    def process_directory(self, input_dir="data/raw", output_file="data/processed/patents.json", max_files=None, max_patents_per_file=50):
        """Procesar todos los archivos XML en directorio - FIXED"""
        
        # Buscar archivos XML
        xml_files = []
        for root, dirs, files in os.walk(input_dir):
            for file in files:
                if file.endswith('.xml'):
                    xml_files.append(os.path.join(root, file))
        
        if not xml_files:
            print(f"No XML files found in {input_dir}")
            return []
        
        if max_files:
            xml_files = xml_files[:max_files]
        
        print(f"Processing {len(xml_files)} XML files...")
        
        all_patents = []
        
        for i, xml_file in enumerate(xml_files):
            print(f"\nFile {i+1}/{len(xml_files)}: {os.path.basename(xml_file)}")
            
            file_patents = self.process_file(xml_file, max_patents_per_file)
            all_patents.extend(file_patents)
            
            print(f"  Extracted {len(file_patents)} patents (Total: {len(all_patents)})")
            
            # Pausa cada 5 archivos para no sobrecargar
            if (i + 1) % 5 == 0:
                print(f"Processed {i+1} files, {len(all_patents)} patents total...")
        
        # FIXED: Crear directorio si no existe
        output_dir = os.path.dirname(output_file)
        if output_dir:  # Solo si hay directorio
            os.makedirs(output_dir, exist_ok=True)
        
        # Guardar resultados
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_patents, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Processing complete!")
        print(f"📁 Total files processed: {len(xml_files)}")
        print(f"📄 Total patents extracted: {len(all_patents)}")
        print(f"💾 Output saved to: {output_file}")
        
        # Crear estadísticas
        if all_patents:  # Solo si hay patentes
            self.create_summary_stats(all_patents, output_dir)
        
        return all_patents
    
    def create_summary_stats(self, patents, output_dir):
        """FIXED: Crear estadísticas del dataset"""
        if not patents:
            return
        
        try:
            df = pd.DataFrame(patents)
            
            stats = {
                "total_patents": len(patents),
                "unique_assignees": df['assignee'].nunique(),
                "top_assignees": df['assignee'].value_counts().head(10).to_dict(),
                "categories": df['category'].value_counts().to_dict(),
                "date_range": {
                    "earliest": df['application_date'].min(),
                    "latest": df['application_date'].max()
                },
                "avg_lengths": {
                    "title": round(df['title'].str.len().mean(), 1),
                    "abstract": round(df['abstract'].str.len().mean(), 1)
                },
                "ipc_distribution": df['ipc_class'].value_counts().head(10).to_dict()
            }

            # FIXED: Usar output_dir correcto
            stats_file = os.path.join(output_dir, "dataset_stats.json") if output_dir else "dataset_stats.json"
            
            with open(stats_file, 'w') as f:
                json.dump(stats, f, indent=2)
            
            print(f"\n📊 DATASET STATISTICS:")
            print(f"   Total Patents: {stats['total_patents']}")
            print(f"   Unique Companies: {stats['unique_assignees']}")
            print(f"   Categories: {list(stats['categories'].keys())}")
            print(f"   Top Companies: {list(stats['top_assignees'].keys())[:5]}")
            
        except Exception as e:
            print(f"⚠️ Error creating stats: {e}")

if __name__ == "__main__":
    parser = USPTOParser()
    
    # Configuración para prueba rápida - ajustar path según ubicación
    current_dir = os.getcwd()
    if current_dir.endswith('backend'):
        input_dir = "../data/raw"
        output_dir = "../data/processed"
    else:
        input_dir = "data/raw"
        output_dir = "data/processed"
    
    print(f"🔍 Looking for XML files in: {os.path.abspath(input_dir)}")
    
    if os.path.exists(input_dir):
        print("🚀 Starting USPTO XML processing...")
        
        # Ajustar output path también
        if current_dir.endswith('backend'):
            output_file = "../data/processed/patents.json"
        else:
            output_file = "data/processed/patents.json"
        
        patents = parser.process_directory(
            input_dir=input_dir,
            output_file=output_file,
            max_files=3,  # Solo 3 archivos para empezar
            max_patents_per_file=100  # Max 100 patentes por archivo
        )
        
        if patents:
            print(f"\n✅ Success! Processed {len(patents)} patents")
            print("Next step: Run the Flask app to load data into Elasticsearch")
        else:
            print("❌ No patents were extracted. Check your XML files.")
    else:
        print(f"❌ Directory {input_dir} not found")
        print("Please ensure your USPTO XML files are in data/raw/")
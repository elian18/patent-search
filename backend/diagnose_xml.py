# backend/diagnose_xml.py
import os
import re

def diagnose_xml_file(file_path):
    """Diagnosticar estructura del archivo XML de USPTO"""
    
    print(f"🔍 Analyzing: {file_path}")
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            # Leer primeras 10000 líneas para análisis
            lines = []
            for i, line in enumerate(f):
                lines.append(line)
                if i > 10000:
                    break
        
        content = ''.join(lines)
        
        print(f"📊 File size: {os.path.getsize(file_path) / (1024*1024):.1f} MB")
        print(f"📝 Lines analyzed: {len(lines)}")
        
        # Buscar patrones XML
        xml_declarations = re.findall(r'<\?xml[^>]*\?>', content)
        print(f"🔖 XML declarations found: {len(xml_declarations)}")
        
        # Buscar elementos de patente
        patent_patterns = [
            ('us-patent-application', r'<us-patent-application[^>]*>'),
            ('patent-application-publication', r'<patent-application-publication[^>]*>'),
            ('us-patent-grant', r'<us-patent-grant[^>]*>'),
            ('invention-title', r'<invention-title[^>]*>'),
            ('abstract', r'<abstract[^>]*>'),
            ('assignee', r'<assignee[^>]*>')
        ]
        
        print("\n📋 Patent-related elements found:")
        for name, pattern in patent_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            print(f"   {name}: {len(matches)}")
        
        # Mostrar primeras líneas
        print(f"\n📄 First 10 lines:")
        for i, line in enumerate(lines[:10]):
            print(f"   {i+1:2d}: {line.strip()}")
        
        # Buscar posibles inicios de documentos
        doc_starts = []
        for i, line in enumerate(lines):
            if ('<?xml' in line or 
                '<us-patent-application' in line or 
                '<patent-application-publication' in line):
                doc_starts.append(i + 1)
        
        print(f"\n🎯 Potential document starts at lines: {doc_starts[:10]}")
        
        if len(doc_starts) > 1:
            print("✅ Multiple documents detected - this is concatenated XML")
        elif len(doc_starts) == 1:
            print("✅ Single document detected")
        else:
            print("❌ No clear document structure found")
        
        # Verificar encoding
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                f.read(1000)
            print("✅ UTF-8 encoding works")
        except:
            print("⚠️ UTF-8 encoding issues - trying latin-1")
    
    except Exception as e:
        print(f"❌ Error analyzing file: {e}")

if __name__ == "__main__":
    # Ajustar path según ubicación
    current_dir = os.getcwd()
    if current_dir.endswith('backend'):
        xml_file = "../data/raw/ipa250731.xml"
    else:
        xml_file = "data/raw/ipa250731.xml"
    
    diagnose_xml_file(xml_file)
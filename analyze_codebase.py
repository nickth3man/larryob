import os
import ast
import json

base_dir = os.path.abspath('.')
src_dir = os.path.join(base_dir, 'src')
tests_dir = os.path.join(base_dir, 'tests')

results = {
    'files': [],
    'dirs': {},
    'classes': [],
    'functions': [],
    'inits': [],
    'unusual': []
}

def analyze_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            source = f.read()
    except Exception:
        return
    
    lines = source.splitlines()
    line_count = len(lines)
    
    dir_name = os.path.dirname(filepath)
    
    if dir_name not in results['dirs']:
        results['dirs'][dir_name] = {'count': 0, 'files': []}
    results['dirs'][dir_name]['count'] += 1
    results['dirs'][dir_name]['files'].append(filepath)
    
    results['files'].append({'path': filepath, 'lines': line_count})
    
    try:
        tree = ast.parse(source, filename=filepath)
    except SyntaxError:
        return
        
    if os.path.basename(filepath) == '__init__.py':
        exports = []
        for node in tree.body:
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    exports.append(alias.name)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == '__all__':
                        if isinstance(node.value, (ast.List, ast.Tuple)):
                            exports.extend([elt.value for elt in node.value.elts if isinstance(elt, ast.Constant)])
        results['inits'].append({'path': filepath, 'exports': exports})
        
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            results['classes'].append({'name': node.name, 'file': filepath})
        elif isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
            results['functions'].append({'name': node.name, 'file': filepath})
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in ['importlib', 'sys']:
                    results['unusual'].append({'type': f'import_{alias.name}', 'file': filepath, 'line': node.lineno})
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in ['__import__', 'eval', 'exec']:
                results['unusual'].append({'type': f'call_{node.func.id}', 'file': filepath, 'line': node.lineno})

if os.path.exists(src_dir):
    for root, dirs, files in os.walk(src_dir):
        for file in files:
            if file.endswith('.py'):
                analyze_file(os.path.join(root, file))
            
if os.path.exists(tests_dir):
    for root, dirs, files in os.walk(tests_dir):
        for file in files:
            if file.endswith('.py'):
                analyze_file(os.path.join(root, file))

print(json.dumps(results, indent=2))

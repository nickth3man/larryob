import os
import sys

def sync_agents():
    project_root = os.getcwd()
    changed = False
    
    # Optimization: explicitly ignore common non-source directories to speed up traversal
    ignored_dirs = {
        '__pycache__', 'node_modules', 'venv', 'env', 'dist', 'build', 'coverage',
        'htmlcov', 'site-packages', 'eggs', '.git', '.hg', '.svn', '.tox',
        '.nox', '.venv', '.mypy_cache', '.pytest_cache', '.ruff_cache'
    }

    # Walk through the project directory
    for root, dirs, files in os.walk(project_root):
        # Optimization: Modify dirs in-place to prune the walk tree
        # Skip hidden directories (starting with .) AND explicitly ignored directories
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ignored_dirs]
        
        if 'AGENTS.md' in files:
            agents_path = os.path.join(root, 'AGENTS.md')
            
            try:
                # Lazy load content and stats to minimize I/O
                agents_content = None
                agents_stat = None
                
                for target_filename in ['GEMINI.md', 'CLAUDE.md']:
                    target_path = os.path.join(root, target_filename)
                    should_write = False
                    
                    if not os.path.exists(target_path):
                        should_write = True
                    else:
                        # Optimization: Check file size first
                        if agents_stat is None:
                            agents_stat = os.stat(agents_path)
                        
                        try:
                            target_stat = os.stat(target_path)
                            if target_stat.st_size != agents_stat.st_size:
                                should_write = True
                            else:
                                # Sizes match, must compare content byte-for-byte
                                if agents_content is None:
                                    with open(agents_path, 'rb') as f:
                                        agents_content = f.read()
                                
                                with open(target_path, 'rb') as f:
                                    target_content = f.read()
                                
                                if agents_content != target_content:
                                    should_write = True
                        except OSError:
                            should_write = True

                    if should_write:
                        # Ensure we have the source content loaded
                        if agents_content is None:
                            with open(agents_path, 'rb') as f:
                                agents_content = f.read()
                                
                        with open(target_path, 'wb') as f:
                            f.write(agents_content)
                        print(f"Updated: {target_path}")
                        changed = True
                        
            except Exception as e:
                print(f"Error processing {root}: {e}")

    if changed:
        print("GEMINI.md and CLAUDE.md files were synchronized with AGENTS.md.")
        print("Please stage the changes and commit again.")
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    sync_agents()

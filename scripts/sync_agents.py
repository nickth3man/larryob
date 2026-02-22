import os
import sys

def sync_agents():
    project_root = os.getcwd()
    changed = False
    
    # Walk through the project directory
    for root, dirs, files in os.walk(project_root):
        # Skip hidden directories like .git, .venv, etc.
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        
        if 'AGENTS.md' in files:
            agents_path = os.path.join(root, 'AGENTS.md')
            
            try:
                with open(agents_path, 'r', encoding='utf-8') as f:
                    agents_content = f.read()
                
                for target_filename in ['GEMINI.md', 'CLAUDE.md']:
                    target_path = os.path.join(root, target_filename)
                    
                    target_content = None
                    if os.path.exists(target_path):
                        with open(target_path, 'r', encoding='utf-8') as f:
                            target_content = f.read()
                    
                    if agents_content != target_content:
                        with open(target_path, 'w', encoding='utf-8') as f:
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

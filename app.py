"""
Ponto de entrada principal e simplificado para o HUB Integrador de Dados - Rodomax Transportes.

Permite a execução direta via:
    py app.py
    python app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Garante que o diretório atual esteja no sys.path
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

from app_interface import RodomaxHubApp

if __name__ == "__main__":
    app = RodomaxHubApp()
    app.mainloop()

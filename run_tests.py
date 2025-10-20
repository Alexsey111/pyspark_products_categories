#!/usr/bin/env python3
"""
Скрипт для запуска тестов PySpark приложения.
"""

import sys
import unittest
from pathlib import Path

# Добавляем корневую директорию в путь для импорта
sys.path.insert(0, str(Path(__file__).parent))

# Импортируем и запускаем тесты
if __name__ == "__main__":
    # Находим все тесты
    loader = unittest.TestLoader()
    start_dir = 'tests'
    suite = loader.discover(start_dir)
    
    # Запускаем тесты
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Возвращаем код выхода
    sys.exit(0 if result.wasSuccessful() else 1)

# PySpark Products and Categories Application

Приложение для работы с продуктами и категориями в PySpark, которое решает задачу получения всех пар "Имя продукта – Имя категории" и продуктов без категорий.

## 📋 Описание задачи

В PySpark приложении датафреймами заданы:
- **Продукты** - каждый продукт может иметь несколько категорий или ни одной
- **Категории** - каждой категории может соответствовать несколько продуктов или ни одного
- **Связи** - таблица связей между продуктами и категориями

**Требуется:** Написать метод, который в одном датафрейме вернет:
1. Все пары «Имя продукта – Имя категории»
2. Имена всех продуктов, у которых нет категорий

## 🏗️ Архитектура

### Основные компоненты

```
pyspark_app/
├── __init__.py
└── products_categories.py
    ├── ProductsCategoriesProcessor
    │   ├── get_products_with_categories()
    │   ├── get_products_without_categories()
    │   └── get_all_products_and_categories()
    └── create_sample_data()
```

### Класс ProductsCategoriesProcessor

Основной класс для обработки данных о продуктах и категориях.

**Основные методы:**

- `get_products_with_categories()` - получает все пары продукт-категория и продукты без категорий
- `get_products_without_categories()` - получает только продукты без категорий
- `get_all_products_and_categories()` - алиас для get_products_with_categories()

## 🚀 Быстрый старт

### Установка зависимостей

```bash
pip install pyspark
```

### Базовое использование

```python
from pyspark.sql import SparkSession
from pyspark_app.products_categories import ProductsCategoriesProcessor, create_sample_data

# Создание Spark сессии
spark = SparkSession.builder \
    .appName("ProductsCategories") \
    .master("local[2]") \
    .getOrCreate()

# Создание процессора
processor = ProductsCategoriesProcessor(spark)

# Создание тестовых данных
products_df, categories_df, product_categories_df = create_sample_data(spark)

# Получение всех пар продукт-категория
result_df = processor.get_products_with_categories(
    products_df, categories_df, product_categories_df
)

# Показ результатов
result_df.show()
```

## 📊 Структура данных

### Входные DataFrame

**products_df** (продукты):
```
+----------+------------+
|product_id|product_name|
+----------+------------+
|         1|      Laptop|
|         2|       Mouse|
|         3|    Keyboard|
+----------+------------+
```

**categories_df** (категории):
```
+------------+--------------+
|category_id |category_name |
+------------+--------------+
|           1|   Electronics|
|           2|Computer Acc. |
+------------+--------------+
```

**product_categories_df** (связи):
```
+----------+------------+
|product_id|category_id |
+----------+------------+
|         1|           1|
|         1|           2|
|         2|           1|
+----------+------------+
```

### Выходной DataFrame

**result_df** (результат):
```
+------------+--------------+
|product_name|category_name |
+------------+--------------+
|      Laptop|   Electronics|
|      Laptop|Computer Acc. |
|       Mouse|   Electronics|
|   Uncategorized|        null|
+------------+--------------+
```

## 🔧 API Reference

### ProductsCategoriesProcessor

#### `get_products_with_categories(products_df, categories_df, product_categories_df)`

**Параметры:**
- `products_df` - DataFrame с продуктами (колонки: product_id, product_name)
- `categories_df` - DataFrame с категориями (колонки: category_id, category_name)
- `product_categories_df` - DataFrame со связями (колонки: product_id, category_id)

**Возвращает:**
- DataFrame с колонками product_name, category_name
- Для продуктов без категорий category_name = null

#### `get_products_without_categories(products_df, product_categories_df)`

**Параметры:**
- `products_df` - DataFrame с продуктами
- `product_categories_df` - DataFrame со связями

**Возвращает:**
- DataFrame с колонкой product_name (только продукты без категорий)

### Вспомогательные функции

#### `create_sample_data(spark)`

Создает примеры данных для тестирования.

**Возвращает:**
- Кортеж (products_df, categories_df, product_categories_df)

## 🧪 Тестирование

### Запуск тестов

```bash
python -m unittest discover tests -v
```

### Покрытие тестами

- ✅ Базовая функциональность
- ✅ Продукты без категорий
- ✅ Валидация входных данных
- ✅ Пустые DataFrame
- ✅ Продукты с несколькими категориями
- ✅ Создание тестовых данных

### Примеры тестов

```python
def test_get_products_with_categories_basic(self):
    """Тест базовой функциональности."""
    products_df, categories_df, product_categories_df = create_sample_data(self.spark)
    
    result_df = self.processor.get_products_with_categories(
        products_df, categories_df, product_categories_df
    )
    
    # Проверки...
```

## 📝 Примеры использования

### Пример 1: Базовое использование

```python
from pyspark.sql import SparkSession
from pyspark_app.products_categories import ProductsCategoriesProcessor

spark = SparkSession.builder.appName("Demo").master("local").getOrCreate()
processor = ProductsCategoriesProcessor(spark)

# Ваши данные
products_df = spark.createDataFrame([
    (1, "Laptop"), (2, "Mouse"), (3, "Uncategorized Item")
], ["product_id", "product_name"])

categories_df = spark.createDataFrame([
    (1, "Electronics"), (2, "Computer Accessories")
], ["category_id", "category_name"])

product_categories_df = spark.createDataFrame([
    (1, 1), (1, 2), (2, 1)  # Laptop в 2 категориях, Mouse в 1
], ["product_id", "category_id"])

# Получение результатов
result = processor.get_products_with_categories(
    products_df, categories_df, product_categories_df
)
result.show()
```

### Пример 2: Только продукты без категорий

```python
products_without_categories = processor.get_products_without_categories(
    products_df, product_categories_df
)
products_without_categories.show()
```

### Пример 3: Сложные SQL запросы

```python
# Создание временных представлений
products_df.createOrReplaceTempView("products")
categories_df.createOrReplaceTempView("categories")
product_categories_df.createOrReplaceTempView("product_categories")

# Продукты с количеством категорий
query = """
SELECT 
    p.product_name,
    COUNT(c.category_name) as category_count
FROM products p
LEFT JOIN product_categories pc ON p.product_id = pc.product_id
LEFT JOIN categories c ON pc.category_id = c.category_id
GROUP BY p.product_id, p.product_name
ORDER BY category_count DESC
"""

result = spark.sql(query)
result.show()
```

## 🏃‍♂️ Запуск демо

```bash
python example_usage.py
```

Демо покажет:
- Создание тестовых данных
- Исходные данные
- Все пары продукт-категория
- Продукты без категорий
- Сложные SQL запросы
- Статистику

## 📋 Требования

- **Python**: 3.7+
- **PySpark**: 3.0+
- **Java**: 8+ (для Spark)

## 🔍 Особенности реализации

### SQL подход
Используется SQL для максимальной производительности и читаемости:
```sql
SELECT 
    p.product_name,
    c.category_name
FROM products p
LEFT JOIN product_categories pc ON p.product_id = pc.product_id
LEFT JOIN categories c ON pc.category_id = c.category_id
ORDER BY p.product_name, c.category_name
```

### Валидация данных
Проверка наличия необходимых колонок в DataFrame:
```python
def _validate_dataframes(self, products_df, categories_df, product_categories_df):
    required_cols = ['product_id', 'product_name']
    missing_cols = [col for col in required_cols if col not in products_df.columns]
    if missing_cols:
        raise ValueError(f"Отсутствуют колонки: {missing_cols}")
```

### Обработка NULL значений
Продукты без категорий корректно отображаются с NULL в category_name.

## 🎯 Решение задачи

Приложение полностью решает поставленную задачу:

1. ✅ **Все пары "Имя продукта – Имя категории"** - метод `get_products_with_categories()`
2. ✅ **Продукты без категорий** - отображаются с NULL в category_name
3. ✅ **Один датафрейм** - все результаты в одном DataFrame
4. ✅ **Тесты** - полное покрытие тестами
5. ✅ **Производительность** - использование SQL для оптимальной работы

## 📄 Лицензия

MIT License

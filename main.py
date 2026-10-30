from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round
from pyspark.sql.types import IntegerType, DoubleType
import os

# Кольори для заголовків етапів у консолі (ANSI-коди)
HEADER = "\033[1;36m"  # жирний бірюзовий
RESET = "\033[0m"      # скидання кольору

# Створюємо локальну сесію Spark (local[*] — на цьому комп'ютері, всі ядра)
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("goit-de-hw-03")
    .getOrCreate()

)


# ---------- 1. Завантаження CSV-файлів у DataFrame ----------
print(f"\n{HEADER}===== 1. Завантаження CSV-файлів у DataFrame ====={RESET}\n")
users_df = spark.read.csv('./users.csv', header=True)
purchases_df = spark.read.csv('./purchases.csv', header=True)
products_df = spark.read.csv('./products.csv', header=True)

print("users:", users_df.count())
users_df.show(5)
print("purchases:", purchases_df.count())
purchases_df.show(5)
print("products:", products_df.count())
products_df.show(5)

input("Крок 1 виконано. Натисніть Enter...")
os.system("clear")  # очищаємо екран терміналу перед наступним кроком


# ---------- 2. Очищення даних: видаляємо рядки з пропущеними значеннями ----------
print(f"\n{HEADER}===== 2. Очищення даних: видаляємо рядки з пропущеними значеннями ====={RESET}\n")
users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

# Приводимо числові колонки до потрібних типів
users_df = users_df.withColumn("age", col("age").cast(IntegerType()))
purchases_df = purchases_df.withColumn("quantity", col("quantity").cast(IntegerType()))
products_df = products_df.withColumn("price", col("price").cast(DoubleType()))

print("users після очищення:", users_df.count())
users_df.show(5)
print("purchases після очищення:", purchases_df.count())
purchases_df.show(5)
print("products після очищення:", products_df.count())
products_df.show(5)

input("Крок 2 виконано. Натисніть Enter...")
os.system("clear")  # очищаємо екран терміналу перед наступним кроком


# ---------- 3. Загальна сума покупок за кожною категорією продуктів ----------
print(f"\n{HEADER}===== 3. Загальна сума покупок за кожною категорією продуктів ====={RESET}\n")
# Поєднуємо покупки з продуктами та рахуємо суму покупки: quantity * price
purchases_products_df = purchases_df \
    .join(products_df, purchases_df.product_id == products_df.product_id, 'inner') \
    .drop(products_df.product_id) \
    .withColumn("amount", col("quantity") * col("price"))

total_by_category_df = purchases_products_df \
    .groupBy("category") \
    .agg(round(sum("amount"), 2).alias("total_amount")) \
    .orderBy(col("total_amount").desc())

total_by_category_df.show()

input("Крок 3 виконано. Натисніть Enter...")
os.system("clear")  # очищаємо екран терміналу перед наступним кроком


# ---------- 4. Сума покупок за категоріями для віку 18-25 включно ----------
print(f"\n{HEADER}===== 4. Сума покупок за категоріями для віку 18-25 включно ====={RESET}\n")
# Додаємо дані користувачів і залишаємо лише вік від 18 до 25
young_df = purchases_products_df \
    .join(users_df, purchases_products_df.user_id == users_df.user_id, 'inner') \
    .drop(users_df.user_id) \
    .where((col("age") >= 18) & (col("age") <= 25))

young_by_category_df = young_df \
    .groupBy("category") \
    .agg(round(sum("amount"), 2).alias("total_amount")) \
    .orderBy(col("total_amount").desc())

young_by_category_df.show()

input("Крок 4 виконано. Натисніть Enter...")
os.system("clear")  # очищаємо екран терміналу перед наступним кроком


# ---------- 5. Частка покупок за категоріями від сумарних витрат (вік 18-25) ----------
print(f"\n{HEADER}===== 5. Частка покупок за категоріями від сумарних витрат (вік 18-25) ====={RESET}\n")
# Загальна сума витрат усіх покупців 18-25
young_total = young_df.agg(sum("amount")).collect()[0][0]
print(f"Сумарні витрати (18-25): {young_total:.2f}")

young_share_df = young_df \
    .groupBy("category") \
    .agg(sum("amount").alias("total_amount")) \
    .withColumn("percent", round(col("total_amount") / young_total * 100, 2)) \
    .withColumn("total_amount", round(col("total_amount"), 2)) \
    .orderBy(col("percent").desc())

young_share_df.show()

input("Крок 5 виконано. Натисніть Enter...")
os.system("clear")  # очищаємо екран терміналу перед наступним кроком


# ---------- 6. Топ-3 категорії з найвищим відсотком витрат (вік 18-25) ----------
print(f"\n{HEADER}===== 6. Топ-3 категорії з найвищим відсотком витрат (вік 18-25) ====={RESET}\n")
top3_df = young_share_df \
    .orderBy(col("percent").desc()) \
    .limit(3)

top3_df.show()

input("Крок 6 виконано. Натисніть Enter...")

# Закриваємо сесію Spark
spark.stop()

import aiosqlite
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init_db(self) -> None:
        """Initialize the database and create necessary tables"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS processed_products (
                    id INTEGER PRIMARY KEY,
                    product_name TEXT,
                    original_data_column_1 TEXT,
                    original_data_column_2 TEXT,
                    supplier_article_number TEXT,
                    ean TEXT,
                    article_number TEXT,
                    product_description TEXT,
                    supplier TEXT,
                    supplier_url TEXT,
                    product_image_url TEXT,
                    manufacturer TEXT,
                    original_data_column_3 TEXT,
                    category TEXT,
                    page TEXT
                )
            """)
            await db.commit()
        logging.info("Database initialized and tables created.")

    async def get_last_state(self) -> tuple[str, int]:
        """Get the last processed category URL and page number.

        Returns
        -------
        tuple[str, int]
            The last category URL and the last processed page number.
        """
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """SELECT category, page
                FROM processed_products
                WHERE id = (SELECT MAX(id) FROM processed_products);
                """
            ) as cursor:
                row = await cursor.fetchone()
        return row if row else (None, 1)

    async def get_processed_products(self, category_code, page_num) -> list[dict]:
        """Return a list of processed product data as dictionaries.

        Returns
        -------
        list[dict]
            List of dictionaries with product data.
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """SELECT product_name,
                original_data_column_1,
                original_data_column_2,
                supplier_article_number,
                ean,
                article_number,
                product_description,
                supplier,
                supplier_url,
                product_image_url,
                manufacturer,
                original_data_column_3
                FROM processed_products
                WHERE category = ? AND page = ?
                """,
                (category_code, page_num),
            ) as cursor:
                rows = await cursor.fetchall()
                if rows:
                    processed_products = [dict(row) for row in rows]
                    logging.info(
                        f"Retrieved processed products: {[product['supplier_url'] for product in processed_products]}, "
                        f"total: {len(processed_products)}"
                    )
                    return processed_products
                else:
                    logging.info("No processed products found.")
                    return []

    async def get_all_from_db(self) -> tuple[list[tuple[str]], list[str]]:
        """Retrieve all product data from the 'processed_products' table.

        Returns
        -------
        tuple[list[tuple[str]], list[str]]
            A tuple containing two elements:
            1. A list of tuples, where each tuple represents a row from the database.
            2. A list of strings, representing the column names of the table.
        """
        query = """SELECT
            product_name, original_data_column_1, original_data_column_2,
            supplier_article_number, ean, article_number, product_description,
            supplier, supplier_url, product_image_url, manufacturer, original_data_column_3
            FROM processed_products
        """

        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(query) as cursor:
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]
        return rows, columns

    async def add_to_processed(
        self, product: dict[str, str], category: str, page: str
    ) -> None:
        """Add a product URL to the processed products table.

        Parameters
        ----------
        product_url : str
            The URL of the product.
        category : str
            The category of the product.
        page : int
            The page number where the product was found.
        """
        product_copy = product.copy()
        product_copy["category"] = category
        product_copy["page"] = page
        placeholders = ", ".join(["?"] * len(product_copy))
        columns = ", ".join(product_copy.keys())
        query = f"INSERT INTO processed_products ({columns}) VALUES ({placeholders})"

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(query, tuple(product_copy.values()))
            await db.commit()
        logging.info(
            f"Added to processed: {product['supplier_url']}, category: {category}, page: {page}"
        )

    async def clear_processed_urls(self) -> None:
        """Delete all records from the processed_products table for a specific category and page.

        Parameters
        ----------
        category : str
            The category of the products.
        page : int
            The page number.
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM processed_products")
            await db.commit()
        logging.info("Cleared processed URLs")

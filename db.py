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
                CREATE TABLE IF NOT EXISTS state (
                    id INTEGER PRIMARY KEY,
                    category_url TEXT,
                    page_num INTEGER
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS processed_products (
                    product_name TEXT,
                    original_data_column_1 TEXT,
                    original_data_column_2 TEXT,
                    supplier_article_number TEXT,
                    ean TEXT,
                    article_number TEXT,
                    product_description TEXT,
                    supplier TEXT,
                    supplier_url PRIMARY KEY,
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
                "SELECT category_url, page_num FROM state WHERE id = 1"
            ) as cursor:
                row = await cursor.fetchone()
                logging.info(f"Retrieved last state: {row}")
                return row if row else (None, 0)

    async def save_state(self, category_url: str, page_num: int) -> tuple[str, int]:
        """Save the current state of processing.

        Parameters
        ----------
        category_url : str
            The URL of the category.
        page_num : int
            The current page number.
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO state (id, category_url, page_num)
                VALUES (1, ?, ?)
            """,
                (category_url, page_num),
            )
            await db.commit()
        logging.info(f"Saved state: {category_url}, page: {page_num}")
        return (category_url, page_num)

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

    async def clear_processed_urls(self, category: str, page: int) -> None:
        """Delete all records from the processed_products table for a specific category and page.

        Parameters
        ----------
        category : str
            The category of the products.
        page : int
            The page number.
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "DELETE FROM processed_products WHERE category = ? AND page = ?",
                (category, page),
            )
            await db.commit()
        logging.info(f"Cleared processed URLs for category: {category}, page: {page}")

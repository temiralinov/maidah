import type { Pool } from "pg";

const ANALYTICS_INDEXES = [
  {
    name: "idx_orders_status_store_company",
    sql: `
      CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_status_store_company
      ON orders ((COALESCE(NULLIF(LOWER(BTRIM(status)), ''), '')), store_id, company_id)
    `,
  },
  {
    name: "idx_orders_store_company",
    sql: `
      CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_store_company
      ON orders (store_id, company_id)
    `,
  },
  {
    name: "idx_order_products_order_id",
    sql: `
      CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_order_products_order_id
      ON order_products (order_id)
    `,
  },
  {
    name: "idx_order_transactions_order_id",
    sql: `
      CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_order_transactions_order_id
      ON order_transactions (order_id)
    `,
  },
];

export const ensureAnalyticsIndexes = async (pool: Pool): Promise<void> => {
  for (const index of ANALYTICS_INDEXES) {
    try {
      await pool.query(index.sql);
    } catch (error) {
      console.error(`db_index_error ${index.name}`, error);
    }
  }
};

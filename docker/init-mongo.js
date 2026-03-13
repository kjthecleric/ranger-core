// =============================================================================
// Ranger Dev: Seed data for MongoDB nosql source testing
// =============================================================================

// Switch to the application database
db = db.getSiblingDB("ranger_dev");

// Create a user for the ranger_dev database
db.createUser({
  user: "ranger",
  pwd: "ranger",
  roles: [{ role: "readWrite", db: "ranger_dev" }],
});

// ---- Products collection ----
db.products.insertMany([
  {
    sku: "WA-001",
    name: "Widget A",
    category: "widgets",
    price: 19.99,
    in_stock: true,
    tags: ["bestseller", "small"],
    specs: { weight_grams: 150, dimensions: { l: 10, w: 5, h: 3 } },
    created_at: new Date("2025-01-15"),
  },
  {
    sku: "WB-002",
    name: "Widget B",
    category: "widgets",
    price: 49.99,
    in_stock: true,
    tags: ["premium"],
    specs: { weight_grams: 320, dimensions: { l: 15, w: 8, h: 6 } },
    created_at: new Date("2025-02-01"),
  },
  {
    sku: "GX-010",
    name: "Gadget X",
    category: "gadgets",
    price: 12.5,
    in_stock: true,
    tags: ["value"],
    specs: { weight_grams: 80, dimensions: { l: 7, w: 4, h: 2 } },
    created_at: new Date("2025-03-10"),
  },
  {
    sku: "GY-011",
    name: "Gadget Y",
    category: "gadgets",
    price: 149.0,
    in_stock: false,
    tags: ["premium", "fragile"],
    specs: { weight_grams: 500, dimensions: { l: 20, w: 12, h: 8 } },
    created_at: new Date("2025-04-22"),
  },
  {
    sku: "GZ-012",
    name: "Gadget Z",
    category: "gadgets",
    price: 34.95,
    in_stock: true,
    tags: [],
    specs: { weight_grams: 200, dimensions: { l: 12, w: 6, h: 4 } },
    created_at: new Date("2025-06-05"),
  },
]);

// ---- Events collection (time-series-like) ----
db.events.insertMany([
  {
    event_type: "page_view",
    user_id: "u-100",
    url: "/products/widget-a",
    ts: new Date(),
    properties: { referrer: "google.com" },
  },
  {
    event_type: "add_to_cart",
    user_id: "u-100",
    url: "/cart",
    ts: new Date(),
    properties: { product: "WA-001", qty: 2 },
  },
  {
    event_type: "page_view",
    user_id: "u-200",
    url: "/",
    ts: new Date(),
    properties: { referrer: "direct" },
  },
  {
    event_type: "purchase",
    user_id: "u-100",
    url: "/checkout/confirm",
    ts: new Date(),
    properties: { order_total: 39.98, items: 1 },
  },
  {
    event_type: "page_view",
    user_id: "u-300",
    url: "/products/gadget-y",
    ts: new Date(),
    properties: { referrer: "twitter.com" },
  },
]);

// Create indexes
db.products.createIndex({ sku: 1 }, { unique: true });
db.products.createIndex({ category: 1 });
db.events.createIndex({ ts: -1 });
db.events.createIndex({ event_type: 1, ts: -1 });

print("✅ MongoDB seed data loaded into ranger_dev");

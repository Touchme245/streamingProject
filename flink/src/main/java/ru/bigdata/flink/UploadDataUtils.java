package ru.bigdata.flink;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class UploadDataUtils {

    public static String convertToDateFormat(String dateString) {
        try {
            SimpleDateFormat inputFormat = new SimpleDateFormat("M/d/yyyy");

            Date date = inputFormat.parse(dateString);

            SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
            return outputFormat.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static java.sql.Date convertToSqlDate(String dateString) {
        try {
            SimpleDateFormat inputFormat = new SimpleDateFormat("M/d/yyyy");
            java.util.Date parsed = inputFormat.parse(dateString);
            return new java.sql.Date(parsed.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static int getOrCreatePetTypeId(String petType) {
        String selectQuery = "SELECT pet_type_id FROM snowflake.dim_pet_types WHERE name = ?";
        Integer id = getIdFromDatabase(selectQuery, petType);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_pet_types (name) VALUES (?) RETURNING pet_type_id";
            return createId(insertQuery, petType);
        }
    }

    // Получить или создать ID для породы питомца
    public static int getOrCreatePetBreedId(String petBreed) {
        String selectQuery = "SELECT pet_breed_id FROM snowflake.dim_pet_breeds WHERE name = ?";
        Integer id = getIdFromDatabase(selectQuery, petBreed);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_pet_breeds (name) VALUES (?) RETURNING pet_breed_id";
            return createId(insertQuery, petBreed);
        }
    }

    // Получить или создать ID для питомца
    public static int getOrCreatePetId(String petName) {
        String selectQuery = "SELECT pet_id FROM snowflake.dim_pets WHERE name = ?";
        Integer id = getIdFromDatabase(selectQuery, petName);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_pets (name) VALUES (?) RETURNING pet_id";
            return createId(insertQuery, petName);
        }
    }

    // Получить или создать ID для страны
    public static int getOrCreateCountryId(String country) {
        String selectQuery = "SELECT country_id FROM snowflake.dim_countries WHERE name = ?";
        Integer id = getIdFromDatabase(selectQuery, country);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_countries (name) VALUES (?) RETURNING country_id";
            return createId(insertQuery, country);
        }
    }

    // Получить или создать ID для города
    public static int getOrCreateCityId(String city) {
        String selectQuery = "SELECT city_id FROM snowflake.dim_cities WHERE name = ?";
        Integer id = getIdFromDatabase(selectQuery, city);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_cities (name) VALUES (?) RETURNING city_id";
            return createId(insertQuery, city);
        }
    }

    // Получить или создать ID для штата
    public static int getOrCreateStateId(String state) {
        String selectQuery = "SELECT state_id FROM snowflake.dim_states WHERE name = ?";
        Integer id = getIdFromDatabase(selectQuery, state);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_states (name) VALUES (?) RETURNING state_id";
            return createId(insertQuery, state);
        }
    }

    // Получить или создать ID для продавца
    public static int getOrCreateSellerId(String sellerEmail) {
        String selectQuery = "SELECT seller_id FROM snowflake.dim_sellers WHERE email = ?";
        Integer id = getIdFromDatabase(selectQuery, sellerEmail);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_sellers (email) VALUES (?) RETURNING seller_id";
            return createId(insertQuery, sellerEmail);
        }
    }

    // Получить или создать ID для магазина
    public static int getOrCreateStoreId(String storeName) {
        String selectQuery = "SELECT store_id FROM snowflake.dim_stores WHERE name = ?";
        Integer id = getIdFromDatabase(selectQuery, storeName);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_stores (name) VALUES (?) RETURNING store_id";
            return createId(insertQuery, storeName);
        }
    }

    // Получить или создать ID для категории продукта
    public static int getOrCreateProductCategoryId(String categoryName) {
        String selectQuery = "SELECT category_id FROM snowflake.dim_product_categories WHERE name = ?";
        Integer id = getIdFromDatabase(selectQuery, categoryName);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_product_categories (name) VALUES (?) RETURNING category_id";
            return createId(insertQuery, categoryName);
        }
    }

    // Получить или создать ID для категории питомца
    public static int getOrCreatePetCategoryId(String petCategoryName) {
        String selectQuery = "SELECT pet_category_id FROM snowflake.dim_pet_categories WHERE name = ?";
        Integer id = getIdFromDatabase(selectQuery, petCategoryName);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_pet_categories (name) VALUES (?) RETURNING pet_category_id";
            return createId(insertQuery, petCategoryName);
        }
    }

    // Получить или создать ID для поставщика
    public static int getOrCreateSupplierId(String supplierEmail) {
        String selectQuery = "SELECT supplier_id FROM snowflake.dim_suppliers WHERE email = ?";
        Integer id = getIdFromDatabase(selectQuery, supplierEmail);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_suppliers (email) VALUES (?) RETURNING supplier_id";
            return createId(insertQuery, supplierEmail);
        }
    }

    // Получить или создать ID для цвета продукта
    public static int getOrCreateColorId(String color) {
        String selectQuery = "SELECT color_id FROM snowflake.dim_colors WHERE name = ?";
        Integer id = getIdFromDatabase(selectQuery, color);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_colors (name) VALUES (?) RETURNING color_id";
            return createId(insertQuery, color);
        }
    }

    // Получить или создать ID для бренда
    public static int getOrCreateBrandId(String brand) {
        String selectQuery = "SELECT brand_id FROM snowflake.dim_brands WHERE name = ?";
        Integer id = getIdFromDatabase(selectQuery, brand);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_brands (name) VALUES (?) RETURNING brand_id";
            return createId(insertQuery, brand);
        }
    }

    // Получить или создать ID для материала
    public static int getOrCreateMaterialId(String material) {
        String selectQuery = "SELECT material_id FROM snowflake.dim_materials WHERE name = ?";
        Integer id = getIdFromDatabase(selectQuery, material);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_materials (name) VALUES (?) RETURNING material_id";
            return createId(insertQuery, material);
        }
    }

    // Получить или создать ID для продукта
    public static int getOrCreateProductId(String productName) {
        String selectQuery = "SELECT product_id FROM snowflake.dim_products WHERE name = ?";
        Integer id = getIdFromDatabase(selectQuery, productName);
        if (id != null) {
            return id;
        } else {
            String insertQuery = "INSERT INTO snowflake.dim_products (name) VALUES (?) RETURNING product_id";
            return createId(insertQuery, productName);
        }
    }

    // Получить или создать ID для клиента
    public static int getOrCreateCustomerId(String customerName) {
        // Разделяем имя и фамилию, если нужно
        String[] nameParts = customerName.split(" ");
        String firstName = nameParts[0]; // Первая часть - имя
        String lastName = nameParts.length > 1 ? nameParts[1] : ""; // Вторая часть - фамилия, если она есть

        // Используем имя и фамилию для поиска
        String selectQuery = "SELECT customer_id FROM snowflake.dim_customers WHERE first_name = ? AND last_name = ?";
        Integer id = getIdFromDatabase(selectQuery, firstName, lastName);

        if (id != null) {
            return id;
        } else {
            // Если не найдено, создаем нового клиента
            String insertQuery = "INSERT INTO snowflake.dim_customers (first_name, last_name) VALUES (?, ?) RETURNING customer_id";
            return createId(insertQuery, firstName, lastName);
        }
    }

    public static Integer getIdFromDatabase(String query, String firstName, String lastName) {
        try (Connection connection = PostgresCommandsExecutor.getConnection();
             PreparedStatement stmt = connection.prepareStatement(query)) {

            stmt.setString(1, firstName);
            stmt.setString(2, lastName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("customer_id");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static int createId(String query, String firstName, String lastName) {
        try (Connection connection = PostgresCommandsExecutor.getConnection();
             PreparedStatement stmt = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

            stmt.setString(1, firstName);
            stmt.setString(2, lastName);

            int affectedRows = stmt.executeUpdate();
            if (affectedRows > 0) {
                try (ResultSet rs = stmt.getGeneratedKeys()) {
                    if (rs.next()) {
                        return rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static Integer getIdFromDatabase(String query, String param) {
        try (Connection connection = PostgresCommandsExecutor.getConnection();
             PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, param);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            System.err.println("Ошибка при запросе ID из базы данных: " + query);
            e.printStackTrace();
        }
        return null;
    }

    public static int createId(String query, String param) {
        try (Connection connection = PostgresCommandsExecutor.getConnection();
             PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, param);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            System.err.println("Ошибка при создании нового ID");
            e.printStackTrace();
        }
        return -1;
    }
}

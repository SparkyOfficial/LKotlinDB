package com.lkotlindb.examples;

import com.lkotlindb.core.LKotlinDB;
import com.lkotlindb.core.Collection;
import com.lkotlindb.core.Document;
import com.lkotlindb.query.Query;
import com.lkotlindb.core.DatabaseStats;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Пример использования LKotlinDB из Java кода
 * Демонстрирует полную совместимость с Java проектами
 */
public class JavaExample {
    
    public static void main(String[] args) {
        // Создаем базу данных
        LKotlinDB db = LKotlinDB.create("./java_example_data");
        
        // Получаем коллекцию
        Collection users = db.getCollection("users");
        
        // Создаем индекс
        users.createIndex("email");
        users.createIndex("age");
        
        // === ДОБАВЛЕНИЕ ДАННЫХ ===
        
        // Создаем пользователя как Map
        Map<String, Object> user1 = new HashMap<>();
        user1.put("name", "John Doe");
        user1.put("email", "john@example.com");
        user1.put("age", 30);
        user1.put("active", true);
        
        String userId1 = users.insert(user1);
        System.out.println("Добавлен пользователь с ID: " + userId1);
        
        // Добавляем еще пользователей
        Map<String, Object> user2 = new HashMap<>();
        user2.put("name", "Jane Smith");
        user2.put("email", "jane@example.com");
        user2.put("age", 25);
        user2.put("active", true);
        
        users.insert(user2);
        
        Map<String, Object> user3 = new HashMap<>();
        user3.put("name", "Bob Johnson");
        user3.put("email", "bob@example.com");
        user3.put("age", 35);
        user3.put("active", false);
        
        users.insert(user3);
        
        // === ПОИСК ДАННЫХ ===
        
        // Поиск по email
        Document foundUser = users.findOne(Query.create().eq("email", "john@example.com"));
        if (foundUser != null) {
            System.out.println("Найден пользователь: " + foundUser.getData());
        }
        
        // Поиск активных пользователей
        List<Document> activeUsers = users.find(Query.create().eq("active", true));
        System.out.println("Активных пользователей: " + activeUsers.size());
        
        // Поиск пользователей старше 25 лет
        List<Document> olderUsers = users.find(
            Query.create()
                .gt("age", 25)
                .sortAsc("age")
        );
        System.out.println("Пользователей старше 25: " + olderUsers.size());
        
        // Поиск по диапазону возраста
        List<Document> middleAgedUsers = users.find(
            Query.create()
                .gte("age", 25)
                .lte("age", 35)
        );
        System.out.println("Пользователей 25-35 лет: " + middleAgedUsers.size());
        
        // === ОБНОВЛЕНИЕ ДАННЫХ ===
        
        // Обновляем возраст пользователя
        Map<String, Object> updateData = new HashMap<>();
        updateData.put("age", 31);
        updateData.put("lastUpdate", System.currentTimeMillis());
        
        boolean updated = users.updateById(userId1, updateData);
        System.out.println("Пользователь обновлен: " + updated);
        
        // === УДАЛЕНИЕ ДАННЫХ ===
        
        // Удаляем неактивных пользователей
        int deleted = users.deleteMany(Query.create().eq("active", false));
        System.out.println("Удалено неактивных пользователей: " + deleted);
        
        // === СТАТИСТИКА ===
        
        long totalUsers = users.count();
        System.out.println("Всего пользователей: " + totalUsers);
        
        // Получаем статистику базы данных
        DatabaseStats stats = db.getStats();
        System.out.println("\n=== СТАТИСТИКА ===");
        System.out.println("Коллекций: " + stats.getCollections());
        System.out.println("Документов: " + stats.getTotalDocuments());
        System.out.println("Размер: " + stats.getDiskSize() + " байт");
        
        // Закрываем базу данных
        db.close();
        
        System.out.println("\nПример завершен успешно!");
    }
}

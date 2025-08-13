package com.lkotlindb

import com.lkotlindb.core.LKotlinDB
import com.lkotlindb.query.Query
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import java.io.File

class LKotlinDBTest {
    
    private lateinit var db: LKotlinDB
    private val testDbPath = "./test_db"
    
    @BeforeEach
    fun setup() {
        // Удаляем тестовую базу если существует
        File(testDbPath).deleteRecursively()
        db = LKotlinDB.create(testDbPath)
    }
    
    @AfterEach
    fun cleanup() {
        db.close()
        File(testDbPath).deleteRecursively()
    }
    
    @Test
    fun testCreateDatabase() {
        assertNotNull(db)
        assertTrue(File(testDbPath).exists())
    }
    
    @Test
    fun testInsertAndFind() {
        val users = db.getCollection("users")
        
        val user = mapOf(
            "name" to "Test User",
            "email" to "test@example.com",
            "age" to 25
        )
        
        val id = users.insert(user)
        assertNotNull(id)
        
        val found = users.findById(id)
        assertNotNull(found)
        assertEquals(id, found!!.id)
    }
    
    @Test
    fun testQuery() {
        val users = db.getCollection("users")
        
        // Добавляем тестовые данные
        users.insert(mapOf("name" to "Alice", "age" to 25))
        users.insert(mapOf("name" to "Bob", "age" to 30))
        users.insert(mapOf("name" to "Charlie", "age" to 35))
        
        // Тестируем запросы
        val youngUsers = users.find(Query.create().lt("age", 30))
        assertEquals(1, youngUsers.size)
        
        val oldUsers = users.find(Query.create().gte("age", 30))
        assertEquals(2, oldUsers.size)
        
        val sortedUsers = users.find(Query.create().sortDesc("age"))
        assertEquals(3, sortedUsers.size)
    }
    
    @Test
    fun testUpdate() {
        val users = db.getCollection("users")
        
        val user = mapOf("name" to "Test User", "age" to 25)
        val id = users.insert(user)
        
        val updated = users.updateById(id, mapOf("age" to 26))
        assertTrue(updated)
        
        val found = users.findById(id)
        assertNotNull(found)
    }
    
    @Test
    fun testDelete() {
        val users = db.getCollection("users")
        
        val user = mapOf("name" to "Test User", "age" to 25)
        val id = users.insert(user)
        
        val deleted = users.deleteById(id)
        assertTrue(deleted)
        
        val found = users.findById(id)
        assertNull(found)
    }
    
    @Test
    fun testIndex() {
        val users = db.getCollection("users")
        
        // Создаем индекс
        val created = users.createIndex("email")
        assertTrue(created)
        
        // Проверяем что индекс создан
        val indexes = users.getIndexes()
        assertTrue(indexes.contains("email"))
        
        // Удаляем индекс
        val dropped = users.dropIndex("email")
        assertTrue(dropped)
    }
    
    @Test
    fun testInMemoryDatabase() {
        val memDb = LKotlinDB.createInMemory()
        val collection = memDb.getCollection("test")
        
        collection.insert(mapOf("test" to "data"))
        assertEquals(1, collection.count())
        
        memDb.close()
    }
}

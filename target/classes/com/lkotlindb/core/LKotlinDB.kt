package com.lkotlindb.core

import com.lkotlindb.storage.StorageEngine
import com.lkotlindb.storage.FileStorageEngine
import com.lkotlindb.query.QueryEngine
import com.lkotlindb.index.IndexManager
import java.io.File
import java.util.concurrent.ConcurrentHashMap

/**
 * Основной класс локальной базы данных LKotlinDB
 * Совместим с Java/Kotlin проектами и Minecraft
 */
class LKotlinDB private constructor(
    private val dbPath: String,
    private val storageEngine: StorageEngine,
    private val queryEngine: QueryEngine,
    private val indexManager: IndexManager
) {
    
    private val collections = ConcurrentHashMap<String, Collection>()
    
    companion object {
        /**
         * Создает новую базу данных или открывает существующую
         */
        @JvmStatic
        fun create(dbPath: String): LKotlinDB {
            val dbDir = File(dbPath)
            if (!dbDir.exists()) {
                dbDir.mkdirs()
            }
            
            val storageEngine = FileStorageEngine(dbPath)
            val indexManager = IndexManager(dbPath)
            val queryEngine = QueryEngine(storageEngine, indexManager)
            
            return LKotlinDB(dbPath, storageEngine, queryEngine, indexManager)
        }
        
        /**
         * Создает in-memory базу данных (для тестов и временных данных)
         */
        @JvmStatic
        fun createInMemory(): LKotlinDB {
            val storageEngine = FileStorageEngine(":memory:")
            val indexManager = IndexManager(":memory:")
            val queryEngine = QueryEngine(storageEngine, indexManager)
            
            return LKotlinDB(":memory:", storageEngine, queryEngine, indexManager)
        }
    }
    
    /**
     * Получает коллекцию по имени или создает новую
     */
    fun getCollection(name: String): Collection {
        return collections.computeIfAbsent(name) { 
            Collection(name, storageEngine, queryEngine, indexManager)
        }
    }
    
    /**
     * Удаляет коллекцию
     */
    fun dropCollection(name: String): Boolean {
        val collection = collections.remove(name)
        return if (collection != null) {
            storageEngine.dropCollection(name)
            indexManager.dropCollectionIndexes(name)
            true
        } else {
            false
        }
    }
    
    /**
     * Получает список всех коллекций
     */
    fun getCollectionNames(): Set<String> {
        return storageEngine.getCollectionNames()
    }
    
    /**
     * Закрывает базу данных и освобождает ресурсы
     */
    fun close() {
        storageEngine.close()
        indexManager.close()
    }
    
    /**
     * Выполняет компактификацию базы данных
     */
    fun compact() {
        storageEngine.compact()
        indexManager.rebuild()
    }
    
    /**
     * Получает статистику базы данных
     */
    fun getStats(): DatabaseStats {
        return DatabaseStats(
            path = dbPath,
            collections = collections.size,
            totalDocuments = collections.values.sumOf { it.count() },
            diskSize = storageEngine.getDiskSize()
        )
    }
}

/**
 * Статистика базы данных
 */
data class DatabaseStats(
    val path: String,
    val collections: Int,
    val totalDocuments: Long,
    val diskSize: Long
)

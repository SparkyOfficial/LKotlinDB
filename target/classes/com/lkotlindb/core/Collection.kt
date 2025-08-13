package com.lkotlindb.core

import com.lkotlindb.storage.StorageEngine
import com.lkotlindb.query.QueryEngine
import com.lkotlindb.query.Query
import com.lkotlindb.index.IndexManager
import com.google.gson.Gson
import com.google.gson.JsonObject
import java.util.*

/**
 * Коллекция документов в базе данных
 */
class Collection internal constructor(
    private val name: String,
    private val storageEngine: StorageEngine,
    private val queryEngine: QueryEngine,
    private val indexManager: IndexManager
) {
    
    private val gson = Gson()
    
    /**
     * Вставляет документ в коллекцию
     */
    fun insert(document: Any): String {
        val id = generateId()
        val jsonDoc = when (document) {
            is String -> document
            is JsonObject -> gson.toJson(document)
            else -> gson.toJson(document)
        }
        
        val doc = Document(id, jsonDoc)
        storageEngine.insert(name, doc)
        indexManager.updateIndexes(name, doc)
        
        return id
    }
    
    /**
     * Вставляет документ с указанным ID
     */
    fun insert(id: String, document: Any): String {
        val jsonDoc = when (document) {
            is String -> document
            is JsonObject -> gson.toJson(document)
            else -> gson.toJson(document)
        }
        
        val doc = Document(id, jsonDoc)
        storageEngine.insert(name, doc)
        indexManager.updateIndexes(name, doc)
        
        return id
    }
    
    /**
     * Находит документ по ID
     */
    fun findById(id: String): Document? {
        return storageEngine.findById(name, id)
    }
    
    /**
     * Находит документы по запросу
     */
    fun find(query: Query): List<Document> {
        return queryEngine.execute(name, query)
    }
    
    /**
     * Находит все документы
     */
    fun findAll(): List<Document> {
        return storageEngine.findAll(name)
    }
    
    /**
     * Находит первый документ по запросу
     */
    fun findOne(query: Query): Document? {
        return queryEngine.execute(name, query).firstOrNull()
    }
    
    /**
     * Обновляет документ по ID
     */
    fun updateById(id: String, document: Any): Boolean {
        val jsonDoc = when (document) {
            is String -> document
            is JsonObject -> gson.toJson(document)
            else -> gson.toJson(document)
        }
        
        val doc = Document(id, jsonDoc)
        val updated = storageEngine.update(name, doc)
        if (updated) {
            indexManager.updateIndexes(name, doc)
        }
        return updated
    }
    
    /**
     * Обновляет документы по запросу
     */
    fun updateMany(query: Query, update: Any): Int {
        val documents = queryEngine.execute(name, query)
        var updated = 0
        
        for (doc in documents) {
            val updatedDoc = applyUpdate(doc, update)
            if (storageEngine.update(name, updatedDoc)) {
                indexManager.updateIndexes(name, updatedDoc)
                updated++
            }
        }
        
        return updated
    }
    
    /**
     * Удаляет документ по ID
     */
    fun deleteById(id: String): Boolean {
        val deleted = storageEngine.delete(name, id)
        if (deleted) {
            indexManager.removeFromIndexes(name, id)
        }
        return deleted
    }
    
    /**
     * Удаляет документы по запросу
     */
    fun deleteMany(query: Query): Int {
        val documents = queryEngine.execute(name, query)
        var deleted = 0
        
        for (doc in documents) {
            if (storageEngine.delete(name, doc.id)) {
                indexManager.removeFromIndexes(name, doc.id)
                deleted++
            }
        }
        
        return deleted
    }
    
    /**
     * Подсчитывает количество документов
     */
    fun count(): Long {
        return storageEngine.count(name)
    }
    
    /**
     * Подсчитывает количество документов по запросу
     */
    fun count(query: Query): Long {
        return queryEngine.execute(name, query).size.toLong()
    }
    
    /**
     * Создает индекс по полю
     */
    fun createIndex(fieldName: String): Boolean {
        return indexManager.createIndex(name, fieldName)
    }
    
    /**
     * Удаляет индекс
     */
    fun dropIndex(fieldName: String): Boolean {
        return indexManager.dropIndex(name, fieldName)
    }
    
    /**
     * Получает список индексов
     */
    fun getIndexes(): Set<String> {
        return indexManager.getIndexes(name)
    }
    
    private fun generateId(): String {
        return UUID.randomUUID().toString()
    }
    
    private fun applyUpdate(document: Document, update: Any): Document {
        val originalJson = gson.fromJson(document.data, JsonObject::class.java)
        val updateJson = when (update) {
            is String -> gson.fromJson(update, JsonObject::class.java)
            is JsonObject -> update
            else -> gson.fromJson(gson.toJson(update), JsonObject::class.java)
        }
        
        // Простое слияние объектов
        for ((key, value) in updateJson.entrySet()) {
            originalJson.add(key, value)
        }
        
        return Document(document.id, gson.toJson(originalJson))
    }
}

/**
 * Документ в базе данных
 */
data class Document(
    val id: String,
    val data: String
) {
    /**
     * Преобразует данные в объект указанного типа
     */
    inline fun <reified T> toObject(): T {
        return Gson().fromJson(data, T::class.java)
    }
    
    /**
     * Получает значение поля как JsonObject
     */
    fun asJsonObject(): JsonObject {
        return Gson().fromJson(data, JsonObject::class.java)
    }
}

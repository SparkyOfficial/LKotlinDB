package com.lkotlindb.storage

import com.lkotlindb.core.Document

/**
 * Интерфейс для движка хранения данных
 */
interface StorageEngine {
    
    /**
     * Вставляет документ в коллекцию
     */
    fun insert(collectionName: String, document: Document)
    
    /**
     * Находит документ по ID
     */
    fun findById(collectionName: String, id: String): Document?
    
    /**
     * Находит все документы в коллекции
     */
    fun findAll(collectionName: String): List<Document>
    
    /**
     * Обновляет документ
     */
    fun update(collectionName: String, document: Document): Boolean
    
    /**
     * Удаляет документ по ID
     */
    fun delete(collectionName: String, id: String): Boolean
    
    /**
     * Подсчитывает количество документов в коллекции
     */
    fun count(collectionName: String): Long
    
    /**
     * Удаляет коллекцию
     */
    fun dropCollection(collectionName: String): Boolean
    
    /**
     * Получает список всех коллекций
     */
    fun getCollectionNames(): Set<String>
    
    /**
     * Закрывает движок хранения
     */
    fun close()
    
    /**
     * Выполняет компактификацию данных
     */
    fun compact()
    
    /**
     * Получает размер данных на диске
     */
    fun getDiskSize(): Long
}

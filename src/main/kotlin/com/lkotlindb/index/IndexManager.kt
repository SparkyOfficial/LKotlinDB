package com.lkotlindb.index

import com.lkotlindb.core.Document
import com.google.gson.Gson
import com.google.gson.JsonObject
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap

/**
 * Менеджер индексов для оптимизации запросов
 */
class IndexManager(private val dbPath: String) {
    
    private val gson = Gson()
    private val indexes = ConcurrentHashMap<String, ConcurrentHashMap<String, Index>>()
    private val isInMemory = dbPath == ":memory:"
    
    init {
        if (!isInMemory) {
            loadExistingIndexes()
        }
    }
    
    /**
     * Создает индекс для поля в коллекции
     */
    fun createIndex(collectionName: String, fieldName: String): Boolean {
        val collectionIndexes = indexes.computeIfAbsent(collectionName) { ConcurrentHashMap() }
        
        if (collectionIndexes.containsKey(fieldName)) {
            return false // Индекс уже существует
        }
        
        val index = Index(fieldName)
        collectionIndexes[fieldName] = index
        
        if (!isInMemory) {
            saveIndex(collectionName, fieldName, index)
        }
        
        return true
    }
    
    /**
     * Удаляет индекс
     */
    fun dropIndex(collectionName: String, fieldName: String): Boolean {
        val collectionIndexes = indexes[collectionName] ?: return false
        val removed = collectionIndexes.remove(fieldName) != null
        
        if (removed && !isInMemory) {
            deleteIndexFile(collectionName, fieldName)
        }
        
        return removed
    }
    
    /**
     * Проверяет существование индекса
     */
    fun hasIndex(collectionName: String, fieldName: String): Boolean {
        return indexes[collectionName]?.containsKey(fieldName) == true
    }
    
    /**
     * Получает список индексов для коллекции
     */
    fun getIndexes(collectionName: String): Set<String> {
        return indexes[collectionName]?.keys?.toSet() ?: emptySet()
    }
    
    /**
     * Обновляет индексы при изменении документа
     */
    fun updateIndexes(collectionName: String, document: Document) {
        val collectionIndexes = indexes[collectionName] ?: return
        
        try {
            val jsonObject = gson.fromJson(document.data, JsonObject::class.java)
            
            for ((fieldName, index) in collectionIndexes) {
                val fieldValue = getFieldValue(jsonObject, fieldName)
                if (fieldValue != null) {
                    index.addEntry(fieldValue, document.id)
                    
                    if (!isInMemory) {
                        saveIndex(collectionName, fieldName, index)
                    }
                }
            }
        } catch (e: Exception) {
            System.err.println("Failed to update indexes for document ${document.id}: ${e.message}")
        }
    }
    
    /**
     * Удаляет документ из индексов
     */
    fun removeFromIndexes(collectionName: String, documentId: String) {
        val collectionIndexes = indexes[collectionName] ?: return
        
        for ((fieldName, index) in collectionIndexes) {
            index.removeEntry(documentId)
            
            if (!isInMemory) {
                saveIndex(collectionName, fieldName, index)
            }
        }
    }
    
    /**
     * Находит документы по индексу
     */
    fun findByIndex(collectionName: String, fieldName: String, value: Any): Set<String> {
        val collectionIndexes = indexes[collectionName] ?: return emptySet()
        val index = collectionIndexes[fieldName] ?: return emptySet()
        
        return index.findDocuments(value)
    }
    
    /**
     * Удаляет все индексы коллекции
     */
    fun dropCollectionIndexes(collectionName: String) {
        indexes.remove(collectionName)
        
        if (!isInMemory) {
            val indexDir = getIndexDir(collectionName)
            try {
                Files.walk(indexDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach { Files.deleteIfExists(it) }
            } catch (e: Exception) {
                // Игнорируем ошибки при удалении
            }
        }
    }
    
    /**
     * Перестраивает все индексы
     */
    fun rebuild() {
        // Очищаем все индексы
        for (collectionIndexes in indexes.values) {
            for (index in collectionIndexes.values) {
                index.clear()
            }
        }
        
        if (!isInMemory) {
            // Сохраняем пустые индексы
            for ((collectionName, collectionIndexes) in indexes) {
                for ((fieldName, index) in collectionIndexes) {
                    saveIndex(collectionName, fieldName, index)
                }
            }
        }
    }
    
    /**
     * Закрывает менеджер индексов
     */
    fun close() {
        if (!isInMemory) {
            // Сохраняем все индексы
            for ((collectionName, collectionIndexes) in indexes) {
                for ((fieldName, index) in collectionIndexes) {
                    saveIndex(collectionName, fieldName, index)
                }
            }
        }
        indexes.clear()
    }
    
    private fun getFieldValue(jsonObject: JsonObject, fieldPath: String): Any? {
        val parts = fieldPath.split(".")
        var current: JsonObject = jsonObject
        
        for (i in 0 until parts.size - 1) {
            val part = parts[i]
            if (!current.has(part) || !current.get(part).isJsonObject) {
                return null
            }
            current = current.getAsJsonObject(part)
        }
        
        val finalPart = parts.last()
        if (!current.has(finalPart)) {
            return null
        }
        
        val element = current.get(finalPart)
        return when {
            element.isJsonPrimitive -> {
                val primitive = element.asJsonPrimitive
                when {
                    primitive.isBoolean -> primitive.asBoolean
                    primitive.isNumber -> primitive.asNumber
                    primitive.isString -> primitive.asString
                    else -> primitive.asString
                }
            }
            element.isJsonNull -> null
            else -> element.toString()
        }
    }
    
    private fun getIndexDir(collectionName: String): java.nio.file.Path {
        return Paths.get(dbPath, "indexes", collectionName)
    }
    
    private fun getIndexFile(collectionName: String, fieldName: String): java.nio.file.Path {
        return getIndexDir(collectionName).resolve("$fieldName.idx")
    }
    
    private fun saveIndex(collectionName: String, fieldName: String, index: Index) {
        try {
            val indexDir = getIndexDir(collectionName)
            Files.createDirectories(indexDir)
            
            val indexFile = getIndexFile(collectionName, fieldName)
            val indexData = index.serialize()
            
            Files.write(indexFile, indexData.toByteArray())
        } catch (e: Exception) {
            System.err.println("Failed to save index $fieldName for collection $collectionName: ${e.message}")
        }
    }
    
    private fun deleteIndexFile(collectionName: String, fieldName: String) {
        try {
            val indexFile = getIndexFile(collectionName, fieldName)
            Files.deleteIfExists(indexFile)
        } catch (e: Exception) {
            System.err.println("Failed to delete index file $fieldName for collection $collectionName: ${e.message}")
        }
    }
    
    private fun loadExistingIndexes() {
        try {
            val dbDir = Paths.get(dbPath)
            if (!Files.exists(dbDir)) return
            
            val indexesDir = dbDir.resolve("indexes")
            if (!Files.exists(indexesDir)) return
            
            Files.list(indexesDir).use { collectionDirs ->
                collectionDirs
                    .filter { Files.isDirectory(it) }
                    .forEach { collectionDir ->
                        val collectionName = collectionDir.fileName.toString()
                        loadCollectionIndexes(collectionName, collectionDir)
                    }
            }
        } catch (e: Exception) {
            System.err.println("Failed to load existing indexes: ${e.message}")
        }
    }
    
    private fun loadCollectionIndexes(collectionName: String, collectionDir: java.nio.file.Path) {
        try {
            val collectionIndexes = indexes.computeIfAbsent(collectionName) { ConcurrentHashMap() }
            
            Files.list(collectionDir).use { indexFiles ->
                indexFiles
                    .filter { Files.isRegularFile(it) && it.toString().endsWith(".idx") }
                    .forEach { indexFile ->
                        try {
                            val fieldName = indexFile.fileName.toString().removeSuffix(".idx")
                            val content = String(Files.readAllBytes(indexFile))
                            
                            val index = Index.deserialize(fieldName, content)
                            collectionIndexes[fieldName] = index
                        } catch (e: Exception) {
                            System.err.println("Failed to load index ${indexFile.fileName}: ${e.message}")
                        }
                    }
            }
        } catch (e: Exception) {
            System.err.println("Failed to load indexes for collection $collectionName: ${e.message}")
        }
    }
}

/**
 * Индекс для быстрого поиска по полю
 */
class Index(private val fieldName: String) {
    
    private val entries = ConcurrentHashMap<String, MutableSet<String>>()
    private val gson = Gson()
    
    /**
     * Добавляет запись в индекс
     */
    fun addEntry(value: Any, documentId: String) {
        val key = value.toString()
        entries.computeIfAbsent(key) { ConcurrentHashMap.newKeySet() }.add(documentId)
    }
    
    /**
     * Удаляет документ из индекса
     */
    fun removeEntry(documentId: String) {
        val keysToRemove = mutableListOf<String>()
        
        for ((key, documentIds) in entries) {
            documentIds.remove(documentId)
            if (documentIds.isEmpty()) {
                keysToRemove.add(key)
            }
        }
        
        for (key in keysToRemove) {
            entries.remove(key)
        }
    }
    
    /**
     * Находит документы по значению
     */
    fun findDocuments(value: Any): Set<String> {
        val key = value.toString()
        return entries[key]?.toSet() ?: emptySet()
    }
    
    /**
     * Очищает индекс
     */
    fun clear() {
        entries.clear()
    }
    
    /**
     * Сериализует индекс в строку
     */
    fun serialize(): String {
        val data = mapOf(
            "fieldName" to fieldName,
            "entries" to entries.mapValues { it.value.toList() }
        )
        return gson.toJson(data)
    }
    
    companion object {
        /**
         * Десериализует индекс из строки
         */
        fun deserialize(fieldName: String, data: String): Index {
            val gson = Gson()
            val index = Index(fieldName)
            
            try {
                val jsonObject = gson.fromJson(data, JsonObject::class.java)
                val entriesObject = jsonObject.getAsJsonObject("entries")
                
                for ((key, value) in entriesObject.entrySet()) {
                    val documentIds = value.asJsonArray.map { it.asString }.toMutableSet()
                    index.entries[key] = ConcurrentHashMap.newKeySet<String>().apply { addAll(documentIds) }
                }
            } catch (e: Exception) {
                System.err.println("Failed to deserialize index: ${e.message}")
            }
            
            return index
        }
    }
}

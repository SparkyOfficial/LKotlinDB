package com.lkotlindb.storage

import com.lkotlindb.core.Document
import com.google.gson.Gson
import com.google.gson.JsonObject
import java.io.*
import java.nio.file.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Файловый движок хранения данных
 * Совместим с Minecraft (использует только стандартные Java API)
 */
class FileStorageEngine(private val dbPath: String) : StorageEngine {
    
    private val gson = Gson()
    private val collections = ConcurrentHashMap<String, MutableMap<String, Document>>()
    private val locks = ConcurrentHashMap<String, ReentrantReadWriteLock>()
    private val isInMemory = dbPath == ":memory:"
    
    init {
        if (!isInMemory) {
            loadExistingCollections()
        }
    }
    
    override fun insert(collectionName: String, document: Document) {
        val lock = getLock(collectionName)
        lock.write {
            val collection = getOrCreateCollection(collectionName)
            collection[document.id] = document
            
            if (!isInMemory) {
                saveDocument(collectionName, document)
            }
        }
    }
    
    override fun findById(collectionName: String, id: String): Document? {
        val lock = getLock(collectionName)
        return lock.read {
            collections[collectionName]?.get(id)
        }
    }
    
    override fun findAll(collectionName: String): List<Document> {
        val lock = getLock(collectionName)
        return lock.read {
            collections[collectionName]?.values?.toList() ?: emptyList()
        }
    }
    
    override fun update(collectionName: String, document: Document): Boolean {
        val lock = getLock(collectionName)
        return lock.write {
            val collection = collections[collectionName] ?: return@write false
            if (collection.containsKey(document.id)) {
                collection[document.id] = document
                
                if (!isInMemory) {
                    saveDocument(collectionName, document)
                }
                true
            } else {
                false
            }
        }
    }
    
    override fun delete(collectionName: String, id: String): Boolean {
        val lock = getLock(collectionName)
        return lock.write {
            val collection = collections[collectionName] ?: return@write false
            val removed = collection.remove(id) != null
            
            if (removed && !isInMemory) {
                deleteDocumentFile(collectionName, id)
            }
            removed
        }
    }
    
    override fun count(collectionName: String): Long {
        val lock = getLock(collectionName)
        return lock.read {
            collections[collectionName]?.size?.toLong() ?: 0L
        }
    }
    
    override fun dropCollection(collectionName: String): Boolean {
        val lock = getLock(collectionName)
        return lock.write {
            collections.remove(collectionName)
            locks.remove(collectionName)
            
            if (!isInMemory) {
                val collectionDir = getCollectionDir(collectionName)
                try {
                    Files.walk(collectionDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach { Files.deleteIfExists(it) }
                    true
                } catch (e: Exception) {
                    false
                }
            } else {
                true
            }
        }
    }
    
    override fun getCollectionNames(): Set<String> {
        return collections.keys.toSet()
    }
    
    override fun close() {
        if (!isInMemory) {
            // Сохраняем все данные перед закрытием
            for ((collectionName, documents) in collections) {
                for (document in documents.values) {
                    saveDocument(collectionName, document)
                }
            }
        }
        collections.clear()
        locks.clear()
    }
    
    override fun compact() {
        if (isInMemory) return
        
        // Перезаписываем все файлы для удаления фрагментации
        for ((collectionName, documents) in collections) {
            val lock = getLock(collectionName)
            lock.write {
                for (document in documents.values) {
                    saveDocument(collectionName, document)
                }
            }
        }
    }
    
    override fun getDiskSize(): Long {
        if (isInMemory) return 0L
        
        return try {
            val dbDir = Paths.get(dbPath)
            if (!Files.exists(dbDir)) return 0L
            
            Files.walk(dbDir)
                .filter { Files.isRegularFile(it) }
                .mapToLong { Files.size(it) }
                .sum()
        } catch (e: Exception) {
            0L
        }
    }
    
    private fun getLock(collectionName: String): ReentrantReadWriteLock {
        return locks.computeIfAbsent(collectionName) { ReentrantReadWriteLock() }
    }
    
    private fun getOrCreateCollection(collectionName: String): MutableMap<String, Document> {
        return collections.computeIfAbsent(collectionName) { ConcurrentHashMap() }
    }
    
    private fun getCollectionDir(collectionName: String): Path {
        return Paths.get(dbPath, "collections", collectionName)
    }
    
    private fun getDocumentFile(collectionName: String, documentId: String): Path {
        return getCollectionDir(collectionName).resolve("$documentId.json")
    }
    
    private fun saveDocument(collectionName: String, document: Document) {
        try {
            val collectionDir = getCollectionDir(collectionName)
            Files.createDirectories(collectionDir)
            
            val documentFile = getDocumentFile(collectionName, document.id)
            val documentData = JsonObject().apply {
                addProperty("id", document.id)
                addProperty("data", document.data)
            }
            
            Files.write(documentFile, gson.toJson(documentData).toByteArray())
        } catch (e: Exception) {
            // В Minecraft лучше не бросать исключения, а логировать
            System.err.println("Failed to save document ${document.id} in collection $collectionName: ${e.message}")
        }
    }
    
    private fun deleteDocumentFile(collectionName: String, documentId: String) {
        try {
            val documentFile = getDocumentFile(collectionName, documentId)
            Files.deleteIfExists(documentFile)
        } catch (e: Exception) {
            System.err.println("Failed to delete document $documentId in collection $collectionName: ${e.message}")
        }
    }
    
    private fun loadExistingCollections() {
        try {
            val dbDir = Paths.get(dbPath)
            if (!Files.exists(dbDir)) return
            
            val collectionsDir = dbDir.resolve("collections")
            if (!Files.exists(collectionsDir)) return
            
            Files.list(collectionsDir).use { collectionDirs ->
                collectionDirs
                    .filter { Files.isDirectory(it) }
                    .forEach { collectionDir ->
                        val collectionName = collectionDir.fileName.toString()
                        loadCollection(collectionName, collectionDir)
                    }
            }
        } catch (e: Exception) {
            System.err.println("Failed to load existing collections: ${e.message}")
        }
    }
    
    private fun loadCollection(collectionName: String, collectionDir: Path) {
        try {
            val collection = getOrCreateCollection(collectionName)
            
            Files.list(collectionDir).use { documentFiles ->
                documentFiles
                    .filter { Files.isRegularFile(it) && it.toString().endsWith(".json") }
                    .forEach { documentFile ->
                        try {
                            val content = String(Files.readAllBytes(documentFile))
                            val jsonObject = gson.fromJson(content, JsonObject::class.java)
                            
                            val id = jsonObject.get("id").asString
                            val data = jsonObject.get("data").asString
                            
                            collection[id] = Document(id, data)
                        } catch (e: Exception) {
                            System.err.println("Failed to load document ${documentFile.fileName}: ${e.message}")
                        }
                    }
            }
        } catch (e: Exception) {
            System.err.println("Failed to load collection $collectionName: ${e.message}")
        }
    }
}

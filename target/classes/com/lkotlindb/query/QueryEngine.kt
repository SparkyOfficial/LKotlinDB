package com.lkotlindb.query

import com.lkotlindb.core.Document
import com.lkotlindb.storage.StorageEngine
import com.lkotlindb.index.IndexManager
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive

/**
 * Движок выполнения запросов
 */
class QueryEngine(
    private val storageEngine: StorageEngine,
    private val indexManager: IndexManager
) {
    
    private val gson = Gson()
    
    /**
     * Выполняет запрос к коллекции
     */
    fun execute(collectionName: String, query: Query): List<Document> {
        // Пытаемся использовать индексы для оптимизации
        val candidates = findCandidatesUsingIndexes(collectionName, query)
            ?: storageEngine.findAll(collectionName)
        
        // Фильтруем документы по условиям
        val filtered = candidates.filter { document ->
            matchesQuery(document, query)
        }
        
        // Применяем сортировку
        val sorted = applySorting(filtered, query.getSortFields())
        
        // Применяем skip и limit
        val skipped = if (query.getSkip() > 0) {
            sorted.drop(query.getSkip())
        } else {
            sorted
        }
        
        return if (query.getLimit() != null) {
            skipped.take(query.getLimit()!!)
        } else {
            skipped
        }
    }
    
    private fun findCandidatesUsingIndexes(collectionName: String, query: Query): List<Document>? {
        val conditions = query.getConditions()
        
        // Ищем условия равенства, для которых есть индексы
        for (condition in conditions) {
            if (condition.type == ConditionType.EQUALS && 
                indexManager.hasIndex(collectionName, condition.field)) {
                
                val documentIds = indexManager.findByIndex(collectionName, condition.field, condition.value)
                return documentIds.mapNotNull { id ->
                    storageEngine.findById(collectionName, id)
                }
            }
        }
        
        return null
    }
    
    private fun matchesQuery(document: Document, query: Query): Boolean {
        val jsonObject = try {
            gson.fromJson(document.data, JsonObject::class.java)
        } catch (e: Exception) {
            return false
        }
        
        return query.getConditions().all { condition ->
            matchesCondition(jsonObject, condition)
        }
    }
    
    private fun matchesCondition(jsonObject: JsonObject, condition: Condition): Boolean {
        val fieldValue = getFieldValue(jsonObject, condition.field)
        
        return when (condition.type) {
            ConditionType.EQUALS -> compareValues(fieldValue, condition.value) == 0
            ConditionType.NOT_EQUALS -> compareValues(fieldValue, condition.value) != 0
            ConditionType.GREATER_THAN -> compareValues(fieldValue, condition.value) > 0
            ConditionType.GREATER_THAN_OR_EQUAL -> compareValues(fieldValue, condition.value) >= 0
            ConditionType.LESS_THAN -> compareValues(fieldValue, condition.value) < 0
            ConditionType.LESS_THAN_OR_EQUAL -> compareValues(fieldValue, condition.value) <= 0
            ConditionType.CONTAINS -> {
                val fieldStr = fieldValue?.toString() ?: ""
                val valueStr = condition.value.toString()
                fieldStr.contains(valueStr, ignoreCase = true)
            }
            ConditionType.STARTS_WITH -> {
                val fieldStr = fieldValue?.toString() ?: ""
                val valueStr = condition.value.toString()
                fieldStr.startsWith(valueStr, ignoreCase = true)
            }
            ConditionType.ENDS_WITH -> {
                val fieldStr = fieldValue?.toString() ?: ""
                val valueStr = condition.value.toString()
                fieldStr.endsWith(valueStr, ignoreCase = true)
            }
            ConditionType.IN -> {
                val values = condition.value as? List<*> ?: return false
                values.any { compareValues(fieldValue, it) == 0 }
            }
            ConditionType.NOT_IN -> {
                val values = condition.value as? List<*> ?: return true
                values.none { compareValues(fieldValue, it) == 0 }
            }
            ConditionType.EXISTS -> {
                val exists = fieldValue != null
                condition.value == exists
            }
        }
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
    
    private fun compareValues(value1: Any?, value2: Any?): Int {
        if (value1 == null && value2 == null) return 0
        if (value1 == null) return -1
        if (value2 == null) return 1
        
        return when {
            value1 is Number && value2 is Number -> {
                value1.toDouble().compareTo(value2.toString().toDoubleOrNull() ?: 0.0)
            }
            value1 is Boolean && value2 is Boolean -> {
                value1.compareTo(value2)
            }
            else -> {
                value1.toString().compareTo(value2.toString(), ignoreCase = true)
            }
        }
    }
    
    private fun applySorting(documents: List<Document>, sortFields: List<SortField>): List<Document> {
        if (sortFields.isEmpty()) return documents
        
        return documents.sortedWith { doc1, doc2 ->
            val json1 = gson.fromJson(doc1.data, JsonObject::class.java)
            val json2 = gson.fromJson(doc2.data, JsonObject::class.java)
            
            for (sortField in sortFields) {
                val value1 = getFieldValue(json1, sortField.field)
                val value2 = getFieldValue(json2, sortField.field)
                
                val comparison = compareValues(value1, value2)
                if (comparison != 0) {
                    return@sortedWith if (sortField.direction == SortDirection.ASC) {
                        comparison
                    } else {
                        -comparison
                    }
                }
            }
            0
        }
    }
}

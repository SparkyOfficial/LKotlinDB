package com.lkotlindb.query

import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive

/**
 * Класс для построения запросов к базе данных
 * Совместим с Minecraft и Java/Kotlin проектами
 */
class Query {
    
    private val conditions = mutableListOf<Condition>()
    private var limitValue: Int? = null
    private var skipValue: Int = 0
    private val sortFields = mutableListOf<SortField>()
    
    /**
     * Добавляет условие равенства
     */
    fun eq(field: String, value: Any): Query {
        conditions.add(Condition(field, ConditionType.EQUALS, value))
        return this
    }
    
    /**
     * Добавляет условие неравенства
     */
    fun ne(field: String, value: Any): Query {
        conditions.add(Condition(field, ConditionType.NOT_EQUALS, value))
        return this
    }
    
    /**
     * Добавляет условие "больше чем"
     */
    fun gt(field: String, value: Any): Query {
        conditions.add(Condition(field, ConditionType.GREATER_THAN, value))
        return this
    }
    
    /**
     * Добавляет условие "больше или равно"
     */
    fun gte(field: String, value: Any): Query {
        conditions.add(Condition(field, ConditionType.GREATER_THAN_OR_EQUAL, value))
        return this
    }
    
    /**
     * Добавляет условие "меньше чем"
     */
    fun lt(field: String, value: Any): Query {
        conditions.add(Condition(field, ConditionType.LESS_THAN, value))
        return this
    }
    
    /**
     * Добавляет условие "меньше или равно"
     */
    fun lte(field: String, value: Any): Query {
        conditions.add(Condition(field, ConditionType.LESS_THAN_OR_EQUAL, value))
        return this
    }
    
    /**
     * Добавляет условие "содержит" (для строк)
     */
    fun contains(field: String, value: String): Query {
        conditions.add(Condition(field, ConditionType.CONTAINS, value))
        return this
    }
    
    /**
     * Добавляет условие "начинается с"
     */
    fun startsWith(field: String, value: String): Query {
        conditions.add(Condition(field, ConditionType.STARTS_WITH, value))
        return this
    }
    
    /**
     * Добавляет условие "заканчивается на"
     */
    fun endsWith(field: String, value: String): Query {
        conditions.add(Condition(field, ConditionType.ENDS_WITH, value))
        return this
    }
    
    /**
     * Добавляет условие "в списке"
     */
    fun `in`(field: String, values: List<Any>): Query {
        conditions.add(Condition(field, ConditionType.IN, values))
        return this
    }
    
    /**
     * Добавляет условие "не в списке"
     */
    fun notIn(field: String, values: List<Any>): Query {
        conditions.add(Condition(field, ConditionType.NOT_IN, values))
        return this
    }
    
    /**
     * Добавляет условие существования поля
     */
    fun exists(field: String): Query {
        conditions.add(Condition(field, ConditionType.EXISTS, true))
        return this
    }
    
    /**
     * Добавляет условие отсутствия поля
     */
    fun notExists(field: String): Query {
        conditions.add(Condition(field, ConditionType.EXISTS, false))
        return this
    }
    
    /**
     * Ограничивает количество результатов
     */
    fun limit(count: Int): Query {
        limitValue = count
        return this
    }
    
    /**
     * Пропускает указанное количество результатов
     */
    fun skip(count: Int): Query {
        skipValue = count
        return this
    }
    
    /**
     * Добавляет сортировку по возрастанию
     */
    fun sortAsc(field: String): Query {
        sortFields.add(SortField(field, SortDirection.ASC))
        return this
    }
    
    /**
     * Добавляет сортировку по убыванию
     */
    fun sortDesc(field: String): Query {
        sortFields.add(SortField(field, SortDirection.DESC))
        return this
    }
    
    // Геттеры для внутреннего использования
    internal fun getConditions(): List<Condition> = conditions
    internal fun getLimit(): Int? = limitValue
    internal fun getSkip(): Int = skipValue
    internal fun getSortFields(): List<SortField> = sortFields
    
    companion object {
        /**
         * Создает новый пустой запрос
         */
        @JvmStatic
        fun create(): Query = Query()
        

    }
}

/**
 * Условие в запросе
 */
data class Condition(
    val field: String,
    val type: ConditionType,
    val value: Any
)

/**
 * Типы условий
 */
enum class ConditionType {
    EQUALS,
    NOT_EQUALS,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    CONTAINS,
    STARTS_WITH,
    ENDS_WITH,
    IN,
    NOT_IN,
    EXISTS
}

/**
 * Поле для сортировки
 */
data class SortField(
    val field: String,
    val direction: SortDirection
)

/**
 * Направление сортировки
 */
enum class SortDirection {
    ASC, DESC
}

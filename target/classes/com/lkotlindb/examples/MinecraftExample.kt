package com.lkotlindb.examples

import com.lkotlindb.core.LKotlinDB
import com.lkotlindb.query.Query

/**
 * Пример использования LKotlinDB в Minecraft проекте
 * Демонстрирует хранение данных игроков, блоков и других игровых объектов
 */
object MinecraftExample {
    
    @JvmStatic
    fun main(args: Array<String>) {
        // Создаем базу данных в папке сервера
        val db = LKotlinDB.create("./minecraft_data")
        
        // Коллекции для разных типов данных
        val players = db.getCollection("players")
        val blocks = db.getCollection("blocks")
        val economy = db.getCollection("economy")
        
        // Создаем индексы для быстрого поиска
        players.createIndex("username")
        players.createIndex("uuid")
        blocks.createIndex("world")
        blocks.createIndex("type")
        economy.createIndex("player")
        
        // === РАБОТА С ИГРОКАМИ ===
        
        // Добавляем игрока
        val playerId = players.insert(PlayerData(
            username = "Steve",
            uuid = "550e8400-e29b-41d4-a716-446655440000",
            level = 25,
            experience = 1250,
            health = 20.0,
            hunger = 18,
            location = Location("world", 100.5, 64.0, 200.3),
            inventory = listOf(
                ItemStack("minecraft:diamond_sword", 1),
                ItemStack("minecraft:bread", 16)
            ),
            lastLogin = System.currentTimeMillis()
        ))
        
        println("Добавлен игрок с ID: $playerId")
        
        // Поиск игрока по имени
        val steve = players.findOne(Query.create().eq("username", "Steve"))
        if (steve != null) {
            val playerData = steve.toObject<PlayerData>()
            println("Найден игрок: ${playerData.username}, уровень: ${playerData.level}")
        }
        
        // Поиск игроков с высоким уровнем
        val highLevelPlayers = players.find(
            Query.create()
                .gte("level", 20)
                .sortDesc("level")
                .limit(10)
        )
        
        println("Игроки с высоким уровнем: ${highLevelPlayers.size}")
        
        // === РАБОТА С БЛОКАМИ ===
        
        // Сохраняем информацию о размещенных блоках
        blocks.insert(BlockData(
            world = "world",
            x = 100,
            y = 64,
            z = 200,
            type = "minecraft:diamond_ore",
            placedBy = "Steve",
            timestamp = System.currentTimeMillis()
        ))
        
        // Поиск всех алмазных руд в мире
        val diamondOres = blocks.find(
            Query.create()
                .eq("world", "world")
                .eq("type", "minecraft:diamond_ore")
        )
        
        println("Найдено алмазных руд: ${diamondOres.size}")
        
        // === ЭКОНОМИЧЕСКАЯ СИСТЕМА ===
        
        // Добавляем баланс игрока
        economy.insert(EconomyData(
            player = "Steve",
            balance = 1000.0,
            transactions = listOf(
                Transaction("earned", 500.0, "mining", System.currentTimeMillis()),
                Transaction("spent", 200.0, "shop", System.currentTimeMillis())
            )
        ))
        
        // Поиск богатых игроков
        val richPlayers = economy.find(
            Query.create()
                .gte("balance", 500.0)
                .sortDesc("balance")
        )
        
        println("Богатые игроки: ${richPlayers.size}")
        
        // === СТАТИСТИКА ===
        
        val stats = db.getStats()
        println("\n=== СТАТИСТИКА БАЗЫ ДАННЫХ ===")
        println("Путь: ${stats.path}")
        println("Коллекций: ${stats.collections}")
        println("Всего документов: ${stats.totalDocuments}")
        println("Размер на диске: ${stats.diskSize} байт")
        
        // Закрываем базу данных
        db.close()
    }
}

// === DATA CLASSES ДЛЯ MINECRAFT ===

data class PlayerData(
    val username: String,
    val uuid: String,
    val level: Int,
    val experience: Int,
    val health: Double,
    val hunger: Int,
    val location: Location,
    val inventory: List<ItemStack>,
    val lastLogin: Long
)

data class Location(
    val world: String,
    val x: Double,
    val y: Double,
    val z: Double
)

data class ItemStack(
    val type: String,
    val amount: Int
)

data class BlockData(
    val world: String,
    val x: Int,
    val y: Int,
    val z: Int,
    val type: String,
    val placedBy: String,
    val timestamp: Long
)

data class EconomyData(
    val player: String,
    val balance: Double,
    val transactions: List<Transaction>
)

data class Transaction(
    val type: String, // "earned" или "spent"
    val amount: Double,
    val reason: String,
    val timestamp: Long
)

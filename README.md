# LKotlinDB

LKotlinDB - это легковесная, встраиваемая, файловая NoSQL база данных, написанная на Kotlin. Она разработана для обеспечения высокой производительности и надежности в Java и Kotlin проектах, с особой совместимостью для сред, таких как Minecraft, где важна простота и отсутствие внешних зависимостей.

## Особенности

- **Встраиваемая**: Не требует отдельного сервера, работает как библиотека внутри вашего приложения.
- **Файловая структура**: Каждый документ хранится в отдельном JSON-файле, что обеспечивает надежность и простоту резервного копирования.
- **Совместимость с Java 8**: Скомпилировано для JVM 1.8, что гарантирует работу в старых Java-проектах и плагинах для Minecraft.
- **Простой API**: Интуитивно понятный интерфейс для работы с коллекциями и документами.
- **Мощные запросы**: Поддержка фильтрации, сортировки и ограничения результатов.
- **Индексация**: Возможность создания индексов для полей для значительного ускорения поиска.
- **In-Memory режим**: Поддержка работы в памяти для тестов или временных данных.

## Сборка проекта

Проект использует Apache Maven. Для сборки выполните следующую команду в корневой директории проекта:

```shell
mvn clean package
```

Скомпилированный JAR-файл будет находиться в директории `target/lkotlin-database-1.0.0.jar`.

## Пример использования

### Kotlin

```kotlin
import com.lkotlindb.core.LKotlinDB
import com.lkotlindb.query.Query

fun main() {
    val db = LKotlinDB.create("./my_database")
    val users = db.getCollection("users")

    users.createIndex("age")

    val userData = mapOf("name" to "Alice", "age" to 30)
    val userId = users.insert(userData)

    val foundUser = users.findById(userId)
    println(foundUser?.data)

    val youngUsers = users.find(Query.create().lt("age", 35))
    println("Found ${youngUsers.size} young users.")

    db.close()
}
```

### Java

```java
import com.lkotlindb.core.LKotlinDB;
import com.lkotlindb.core.Collection;
import com.lkotlindb.core.Document;
import com.lkotlindb.query.Query;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        LKotlinDB db = LKotlinDB.create("./my_java_db");
        Collection users = db.getCollection("users");

        Map<String, Object> userData = new HashMap<>();
        userData.put("name", "John");
        userData.put("age", 40);

        String userId = users.insert(userData);

        Document foundUser = users.findOne(Query.create().eq("name", "John"));
        if (foundUser != null) {
            System.out.println(foundUser.getData());
        }

        db.close();
    }
}
```

## Поддержка и сообщество

Sparky's Community: https://discord.gg/gz8KUkWWMj
YouTube: https://www.youtube.com/@SparkyOfficialYouTube
Support: donationalerts.com/r/sparky12341234 / donatello.to/Sparky

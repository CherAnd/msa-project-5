**Подсказки по подготовке окружения**

**Подготовка окружения**

1. Установите Docker и Docker Compose
2. Для удобства просмотра кода может понадобиться:
   - установить JDK 17
   - установить Gradle (или используйте встроенную в Idea)
   - установите Idea

   **Сборка приложения**

   ```
   ./gradlew build
   ```


**Создание образа**

   ```
   docker build . -t batch-processin
   ```


**Запуск приложения**

   ```bash
   docker-compose up 
   ```

При запуске приложения таблицы в БД заполняются из sql-файла *task-4/initial/src/main/resources/schema-all.sql*

Получаемые компоненты:
- PostgreSQL (порт 5432)
- batch-processing  

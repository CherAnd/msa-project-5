**Подсказки по подготовке окружения**

1. Установите Docker и Docker Compose
2. Для удобства просмотра кода может понадобиться:

   a. Установите JDK 17
   b. Установите Gradle

   **Сборка приложения**

   ```
   ./gradlew build
   ```

**Создание образа**

   ```
   docker build . -t batch-processing
   ```

**Запуск приложения**

   ```bash
   docker-compose up 
   ```

При запуске приложения в БД автоматически импортируется *task-5/initial/src/main/resources/schema-all.sql*




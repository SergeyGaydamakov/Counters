// Подключение к другому серверу и базе данных
const otherServer = "127.0.0.1";
const otherPort = 27020;
const otherDatabase = "test";
const username = ""; // оставьте пустым если аутентификация не нужна
const password = ""; // оставьте пустым если аутентификация не нужна

// Формируем строку подключения
let connectionString;
if (username && password) {
    connectionString = `mongodb://${username}:${password}@${otherServer}:${otherPort}/${otherDatabase}`;
} else {
    connectionString = `mongodb://${otherServer}:${otherPort}/${otherDatabase}`;
}

print(`Подключение к: ${connectionString}`);

// Подключаемся к базе данных
const client = new Mongo(connectionString);
const db = client.getDB(otherDatabase);

print(`Успешно подключены к базе данных: ${otherDatabase}`);
print(`Доступные коллекции:`);
db.getCollectionNames().forEach(collection => print(`  - ${collection}`));

// Можно выполнить дополнительные команды
// db.stats()
// db.runCommand({listCollections: 1})

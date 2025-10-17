load("base.js");

function ExtractFirstKeyOfIndex(tag) {
  var res = "";
  for (var key in tag) {
    res = key + ": " + JSON.stringify(tag[key]);
    break;
  }
  return res;
}

// Вывод информации о медленных запросах
// detail - степень детализации вывода
function SlowRequests(detail = 1, limit = 10, lastSeconds = 60){
//  const result = db.log.find({c: {$gte: new Date( Date.now() - 1000*60*10)}}, {_id: 1, "t.total": 1}).sort({t:-1}).limit(2).toArray();
  const result = db.log.find({c: {$gte: new Date( Date.now() - 1000*lastSeconds)}}).sort({t:-1}).limit(limit).toArray();
  print(`| ${StrLeftAlign("Time", 20)} | ${StrLeftAlign("_id", 30)} | ${StrRightAlign("t.total, ms", 15)} | ${StrRightAlign("t.counters, ms", 15)} | ${StrRightAlign("t.saveIndex, ms", 15)} | ${StrRightAlign("t.saveFact, ms", 15)} |`);
  result.forEach(function (item) {
    if (!item.t) {
      return;
    }
    print(`| ${StrLeftAlign(DateTimeToString(item.c), 20)} | ${StrLeftAlign(item._id, 30)} | ${StrRightAlign(item.t.total, 15)} | ${StrRightAlign(item.t.counters, 15)} | ${StrRightAlign(item.t.saveIndex, 15)} | ${StrRightAlign(item.t.saveFact, 15)} |`);
  });
  if (detail > 1) {
    print("  " + JSON.stringify(result[0], null, 2));
  }
}

function ShardingAnalysis() {
  DATABASE_NAMES.forEach(function (databaseName) {
    print("*************************************************************************");
    print("***");
    print("***   Анализ шардов для базы данных <" + databaseName + ">");
    print("***");
    print("*************************************************************************");
    ShardingAnalysisForDatabase(databaseName);

  });
  print("");
  print("*************************************************************************");
  print("");
  print("Текущий статус getBalancerState: " + sh.getBalancerState());
  var balanceResult = sh.isBalancerRunning();
  print("Текущий статус isBalancerRunning: " + (balanceResult ? balanceResult.inBalancerRound : "Неизвестно sh.isBalancerRunning()."));
}

// Анализ для одной базы данных
function ShardingAnalysisForDatabase(databaseName = "counters") {
  var config = db.getSiblingDB("config");
  var dbShards = db.adminCommand({ listShards: 1 }).shards;
  var pattern = databaseName + "\..*";
  var count = 0;
  // Заголовок таблицы
  print("");
  print("***   Распределение chunks по зонам шардов");
  print(ArrayToRow([
    StrCenterAlign("Collection name", 45),
    StrCenterAlign("Tag", 20),
    StrCenterAlign("Min", 30),
    StrCenterAlign("Max", 30),
  ]));
  config.tags.find({ "ns": { $regex: pattern, $options: "i" } }).sort({ "ns": 1, "min": 1 }).forEach(function (tag) {
    print(ArrayToRow([
      StrLeftAlign(tag["ns"], 45),
      StrLeftAlign(tag["tag"], 20),
      StrLeftAlign(ExtractFirstKeyOfIndex(tag["min"]), 30),
      StrLeftAlign(ExtractFirstKeyOfIndex(tag["max"]), 30),
    ]));
  });
  // Заголовок таблицы
  print("");
  print("");
  print("***   Число chunks по шардам");
  dbShards.forEach(function (shard) {
    print(shard._id + ": " + (shard.tags && shard.tags.length ? shard.tags : "none") + (shard.draining ? " - draining" : ""));
  });
  print("");
  print(ArrayToRow([
    StrLeftAlign("Collection name:", 50)
  ].concat(dbShards.map(shard => StrCenterAlign(shard._id, 10)))
  ));
  config.chunks.distinct("ns", { "ns": { $regex: pattern, $options: "i" } }).forEach(function (namespace) {
    print(ArrayToRow([
      StrLeftAlign(namespace, 50)
    ].concat(dbShards.map(shard => StrRightAlign(
      config.chunks.find({ "ns": namespace, "shard": shard._id }).count(),
      10
    )))
    ));
  });
  //Jumbo chunks
  print("");
  var jumboChunks = db.getSiblingDB("config").chunks.find({ "ns": { $regex: pattern, $options: "i" }, jumbo: true });
  if (!jumboChunks.hasNext()) {
    print("***   Jumbo chunks not exists. It is OK.");
  } else {
    print("***   Found Jumbo chunks:");
    print(ArrayToRow([
      StrCenterAlign("ns", 45),
      StrCenterAlign("type", 5),
      StrCenterAlign("key", 60),
    ]));
    jumboChunks.forEach(function (chunk) {
      print(ArrayToRow([
        StrLeftAlign(chunk.ns, 45),
        StrCenterAlign("min", 5),
        StrLeftAlign(JSON.stringify(chunk.min), 60)
      ]));
      print(ArrayToRow([
        StrLeftAlign("", 45),
        StrCenterAlign("max", 5),
        StrLeftAlign(JSON.stringify(chunk.max), 60)
      ]));
      print(ArrayToRow([
        StrLeftAlign("", 45),
        StrCenterAlign("", 5),
        StrLeftAlign("", 60)
      ]));
    });
    print("");
    print("Split this chunks by hands: https://docs.mongodb.com/manual/tutorial/clear-jumbo-flag/");
  }
  print("");
  const shardedDataDistribution = sh.getShardedDataDistribution();
  print("Sharded data distribution: ");
  shardedDataDistribution.forEach(coll => {
    if (!coll.ns.includes(databaseName)) {
      return;
    }
    print(`Collection <${coll.ns}> in shards: ${coll.shards.map(shard => shard.shardName + "(" + shard.numOwnedDocuments + " documents)").join(", ")}`);
  });
}

// Статистика для теста counters
// CounterStatistics(1000, 1, 400)
function CounterStatistics(limit = 1000, hashIndex = 10, lastDays = 14, databaseName = "counters") {
  const aggregateOptions = {
    readConcern: { level: "local" },
    readPreference: { mode: "secondaryPreferred" },
    comment: "Statistics for Counter Test - aggregate",
  };
  const findOptions = {
    batchSize: 5000,
    readConcern: { level: "local" },
    readPreference: { mode: "secondaryPreferred" },
    comment: "Statistics for Counter Test - find",
  };
  const dbStat = db.getSiblingDB(databaseName);
  print("***   Statistics for Counter Test");
  print("***   Total number of facts: " + dbStat.facts.estimatedDocumentCount());
  print("***   Total number of factIndex: " + dbStat.factIndex.estimatedDocumentCount());
  // Подсчитываем скорость вставки фактов за последние 10 секунд
  const speedInsertionEndTime = new Date();
  const speedInsertionStartTime = new Date(speedInsertionEndTime.getTime() - 1000 * 10);
  const factsCount = dbStat.facts.find({ c: { $gte: speedInsertionStartTime, $lte: speedInsertionEndTime } }).count();
  print("***   Speed of facts insertion: " + Math.round(factsCount / (speedInsertionEndTime.getTime() - speedInsertionStartTime.getTime()) * 1000) + " facts per second");
  print("");
  const DEFAULT_HASH_1_F1 = "6zymj/oRPwjdpG9Q4VZVwnWkvto=";
  const DEFAULT_HASH_1_F2 = "Zpi8KN0iCSzhoN5pp6xeIpMN0ps=";
  const DEFAULT_HASH_1_F3 = "+OTE0RMF2d9hS6KKy68DpnlmTVk=";
  const DEFAULT_HASH_1_F4 = "TNEtoEkRLcwh9qopA0DH3pP9ilQ=";
  const DEFAULT_HASH_1_F5 = "n/nGzb4PpLzk8K8E48OW4Vd1FqM=";
  const DEFAULT_HASH_1_F6 = "EzHqnTUxxU91DumSj1DbkcHgg4A=";
  const DEFAULT_HASH_1_F7 = "mxUc1Z7KTr8vnwODFP6yYQPw3J8=";
  const DEFAULT_HASH_1_F8 = "0eifnECwHpd6sSIsIlzHZVzaz6I=";
  const DEFAULT_HASH_1_F9 = "cANqv5kHmORkhDgr3a8E7rB/h4E=";
  const DEFAULT_HASH_1_F10 = "NycHIG3BAxGz6oQY/idKelrc0wU=";
  const DEFAULT_HASHES_1 = [
    DEFAULT_HASH_1_F1,
    DEFAULT_HASH_1_F2,
    DEFAULT_HASH_1_F3,
    DEFAULT_HASH_1_F4,
    DEFAULT_HASH_1_F5,
    DEFAULT_HASH_1_F6,
    DEFAULT_HASH_1_F7,
    DEFAULT_HASH_1_F8,
    DEFAULT_HASH_1_F9,
    DEFAULT_HASH_1_F10
  ];
  const hash = DEFAULT_HASHES_1[hashIndex - 1];
  print(`***   Hash index type ${hashIndex}: ${hash}`);
  print("");
  const query = {
    "_id.h": {
      "$in": [
        hash,
        "ec112d7c8244ddedf4f48fbf35c578b33a382793",
        "f970e3072069e8a3e501697f21b275c1bcdb0f6d",
        "2364310307c0cb726405353d065bad33fc845545",
        "c927e3a79298bbd4e94b4da2c04f51ee04216178",
        "bc98caa563ee3afd196167add997c65f6fe5fa98",
        "aa2e8fe36480eaf26ab9fc47fee56c2e4626c9bf",
        "979160d2761537d10d933ebdf64961ba4b68dc17",
        "64a1754a2fa31186873e8231eda5b24e4df0826f",
        "9fa0d762de9b5ce05ee918d180c36b7de38c98f4"
      ]
    },
    "d": {
      "$gte": new Date(Date.now() - 1000 * 3600 * 24 * lastDays)
    }
  };
  // Тестирование скорости выполнения запроса
  const aggregationQueryFacts = [
    {
      "$match": query
    },
    {
      "$limit": limit
    },
    {
      "$group": {
        "_id": "$_id.f"
      }
    },
    {
      "$match": {
        "_id": {
          "$ne": "6ead1ede6d2b8ae568da8b0a"
        }
      }
    },
    {
      "$lookup": {
        "from": "facts",
        "localField": "_id",
        "foreignField": "_id",
        "as": "fact"
      }
    },
    {
      "$unwind": "$fact"
    },
    {
      "$project": {
        "_id": 0,
        "fact": 1,
      }
    }
  ];
  const aggregationStartTime = new Date();
  // print("***   Aggregation query facts: \n" + JSON.stringify(aggregationQueryFacts));
  const result = dbStat.factIndex.aggregate(aggregationQueryFacts, findOptions).batchSize(2000);
  const aggregationEndTime = new Date();
  const arrayResult = result.toArray();
  const aggregationArrayEndTime = new Date();

  print("***   Speed of aggregation query facts: " + Math.round(aggregationEndTime.getTime() - aggregationStartTime.getTime()) + " msec");
  print("***   Speed of get array facts: " + Math.round(aggregationArrayEndTime.getTime() - aggregationEndTime.getTime()) + " msec");
  print("***   Length of result: " + arrayResult.length + " documents, size of result: " + JSON.stringify(arrayResult).length + " Bytes");
  print("");

  const statisticStage = {
    "$facet": {
      "factsTotal": [
        {
          "$group": {
            "_id": null,
            "count": {
              "$sum": 1
            },
            "sumA": {
              "$sum": "$fact.d.a"
            }
          }
        }
      ],
      "factsLastWeek": [
        {
          "$match": {
            "fact.d.dt": {
              "$gte": new Date(Date.now() - 1000 * 3600 * 24 * 7)
            }
          }
        },
        {
          "$group": {
            "_id": null,
            "count": { "$sum": 1 },
            "sumA": { "$sum": "$fact.d.a" }
          }
        }
      ],
      "factsLastHour": [
        {
          "$match": {
            "fact.d.dt": {
              "$gte": new Date(Date.now() - 1000 * 3600)
            }
          }
        },
        {
          "$group": {
            "_id": null,
            "count": { "$sum": 1 },
            "sumA": { "$sum": "$fact.d.a" }
          }
        }
      ],
      "factsLastDay": [
        {
          "$match": {
            "fact.d.dt": {
              "$gte": new Date(Date.now() - 1000 * 3600 * 24)
            }
          }
        },
        {
          "$group": {
            "_id": null,
            "count": { "$sum": 1 },
            "sumA": { "$sum": "$fact.d.a" }
          }
        }
      ],
      "sumALastHour": [
        {
          "$match": {
            "fact.d.dt": {
              "$gte": new Date(Date.now() - 1000 * 3600)
            },
            "fact.d.a": {
              "$gt": 100000
            }
          }
        },
        {
          "$group": {
            "_id": null,
            "totalSum": {
              "$sum": "$fact.d.a"
            }
          }
        }
      ]
    }
  };
  // Тестирование скорости выполнения запроса со статистикой
  const aggregationQueryWithStatistics = aggregationQueryFacts.concat([statisticStage]);
  // print("***   Aggregation query with statistics: \n" + JSON.stringify(aggregationQueryWithStatistics));
  const statisticsAggregationStartTime = new Date();
  const resultStatistics = dbStat.factIndex.aggregate(aggregationQueryWithStatistics, findOptions).batchSize(5000);
  const statisticsAggregationEndTime = new Date();
  const statisticsArrayResult = resultStatistics.toArray();
  const statisticsAggregationArrayEndTime = new Date();

  print("***   Speed of aggregation query with hash index type " + hashIndex + ": " + Math.round(statisticsAggregationEndTime.getTime() - statisticsAggregationStartTime.getTime()) + " msec");
  print("***   Speed of get array result: " + Math.round(statisticsAggregationArrayEndTime.getTime() - statisticsAggregationEndTime.getTime()) + " msec");
  print("***   Length of result: " + statisticsArrayResult.length + " documents, size of result: " + JSON.stringify(statisticsArrayResult).length + " Bytes");
  // Вывод новых статистик
  if (statisticsArrayResult.length > 0) {
    const stats = statisticsArrayResult[0];

    // Количество и сумма фактов всего
    const factsTotal = stats.factsTotal && stats.factsTotal.length > 0 ? stats.factsTotal[0] : { count: 0, sumA: 0 };
    print("***   Facts count for total: " + factsTotal.count + ", sum of field 'a': " + factsTotal.sumA);

    // Количество и сумма фактов за последнюю неделю
    const factsLastWeek = stats.factsLastWeek && stats.factsLastWeek.length > 0 ? stats.factsLastWeek[0] : { count: 0, sumA: 0 };
    print("***   Facts count for last week: " + factsLastWeek.count + ", sum of field 'a': " + factsLastWeek.sumA);

    // Количество и сумма фактов за последний час
    const factsLastHour = stats.factsLastHour && stats.factsLastHour.length > 0 ? stats.factsLastHour[0] : { count: 0, sumA: 0 };
    print("***   Facts count for last hour: " + factsLastHour.count + ", sum of field 'a': " + factsLastHour.sumA);

    // Количество и сумма фактов за последние сутки
    const factsLastDay = stats.factsLastDay && stats.factsLastDay.length > 0 ? stats.factsLastDay[0] : { count: 0, sumA: 0 };
    print("***   Facts count for last day: " + factsLastDay.count + ", sum of field 'a': " + factsLastDay.sumA);

    // Сумма поля 'a' для фактов за последний час с суммой больше 100000
    const sumALastHour = stats.sumALastHour && stats.sumALastHour.length > 0 ? stats.sumALastHour[0].totalSum : 0;
    print("***   Sum of field 'a' for facts in last hour (a > 100000): " + sumALastHour);
  }
  print("");
  // Два отдельных запроса для проверки
  const findQuery = query;
  findQuery["_id.f"] = {
    "$ne": "6ead1ede6d2b8ae568da8b0a"
  };
  // print("***   Find query factIndex: \n" + JSON.stringify(findQuery));
  const findStartTime = new Date();
  const findOptionsForStatistics = {
    batchSize: 5000,
    readConcern: { level: "local" },
    readPreference: { mode: "secondaryPreferred" },
    comment: "getRelevantFactCounters - find",
    projection: { "_id": 1 }
  };
  const cursorfactIndex = dbStat.factIndex.find(findQuery, findOptionsForStatistics).sort({ d: -1 }).limit(limit).batchSize(2000);
  const findEndTime = new Date();
  const findArrayResult = cursorfactIndex.toArray();
  const findArrayEndTime = new Date();
  print("***   Speed of find query factIndex: " + Math.round(findEndTime.getTime() - findStartTime.getTime()) + " msec");
  print("***   Speed of get array factIndex: " + Math.round(findArrayEndTime.getTime() - findEndTime.getTime()) + " msec");
  print("***   Number of factIndex: " + findArrayResult.length + " documents, size of result: " + JSON.stringify(findArrayResult).length + " Bytes");
  print("");
  const queryFacts = {
    "_id": {
      "$in": findArrayResult.map(item => item._id.f)
    }
  };

  // print("***   Find query facts: \n" + JSON.stringify(queryFacts));
  const findFactStartTime = new Date();
  const cursorFacts = dbStat.facts.find(queryFacts).batchSize(2000);
  const findFactEndTime = new Date();
  const findFactArrayResult = cursorFacts.toArray();
  const findFactArrayEndTime = new Date();
  print("***   Speed of find query facts: " + Math.round(findFactEndTime.getTime() - findFactStartTime.getTime()) + " msec");
  print("***   Speed of get array facts: " + Math.round(findFactArrayEndTime.getTime() - findFactEndTime.getTime()) + " msec");
  print("***   Number of facts: " + findFactArrayResult.length + " documents, size of result: " + JSON.stringify(findFactArrayResult).length + " Bytes");
  print("");

  // Запрос статистики через aggregation
  const statisticStageFacts = {
    "$facet": {
      "factsTotal": [
        {
          "$group": {
            "_id": null,
            "count": { "$sum": 1 },
            "sumA": { "$sum": "$d.a" }
          }
        }
      ],
      "factsLastWeek": [
        {
          "$match": {
            "d.dt": {
              "$gte": new Date(Date.now() - 1000 * 3600 * 24 * 7)
            }
          }
        },
        {
          "$group": {
            "_id": null,
            "count": { "$sum": 1 },
            "sumA": { "$sum": "$d.a" }
          }
        }
      ],
      "factsLastHour": [
        {
          "$match": {
            "d.dt": {
              "$gte": new Date(Date.now() - 1000 * 3600)
            }
          }
        },
        {
          "$group": {
            "_id": null,
            "count": { "$sum": 1 },
            "sumA": { "$sum": "$d.a" }
          }
        }
      ],
      "factsLastDay": [
        {
          "$match": {
            "d.dt": {
              "$gte": new Date(Date.now() - 1000 * 3600 * 24)
            }
          }
        },
        {
          "$group": {
            "_id": null,
            "count": { "$sum": 1 },
            "sumA": { "$sum": "$d.a" }
          }
        }
      ],
      "sumALastHour": [
        {
          "$match": {
            "d.dt": {
              "$gte": new Date(Date.now() - 1000 * 3600)
            },
            "d.a": {
              "$gt": 100000
            }
          }
        },
        {
          "$group": {
            "_id": null,
            "totalSum": {
              "$sum": "$d.a"
            }
          }
        }
      ]
    }
  };
  const statisticsFindQuery = [
    {
      "$match": queryFacts
    },
    statisticStageFacts
  ];
  // print("***   Aggregation query for facts statistics: \n" + JSON.stringify(statisticsFindQuery));
  const statisticsFindStartTime = new Date();
  const resultFactsStatistics = dbStat.facts.aggregate(statisticsFindQuery, aggregateOptions);

  const statisticsFindEndTime = new Date();
  const statisticsFindArrayResult = resultFactsStatistics.toArray();
  const statisticsFindArrayEndTime = new Date();
  print("***   Speed of aggregation query for facts statistics: " + Math.round(statisticsFindEndTime.getTime() - statisticsFindStartTime.getTime()) + " msec");
  print("***   Speed of get array facts statistics: " + Math.round(statisticsFindArrayEndTime.getTime() - statisticsFindEndTime.getTime()) + " msec");
  print("***   Length of result: " + statisticsFindArrayResult.length + " documents, size of result: " + JSON.stringify(statisticsFindArrayResult).length + " Bytes");
  // Вывод новых статистик
  if (statisticsFindArrayResult.length > 0) {
    const stats = statisticsFindArrayResult[0];

    // Количество и сумма фактов всего
    const factsTotal = stats.factsTotal && stats.factsTotal.length > 0 ? stats.factsTotal[0] : { count: 0, sumA: 0 };
    print("***   Facts count for total: " + factsTotal.count + ", sum of field 'a': " + factsTotal.sumA);

    // Количество и сумма фактов за последнюю неделю
    const factsLastWeek = stats.factsLastWeek && stats.factsLastWeek.length > 0 ? stats.factsLastWeek[0] : { count: 0, sumA: 0 };
    print("***   Facts count for last week: " + factsLastWeek.count + ", sum of field 'a': " + factsLastWeek.sumA);

    // Количество и сумма фактов за последний час
    const factsLastHour = stats.factsLastHour && stats.factsLastHour.length > 0 ? stats.factsLastHour[0] : { count: 0, sumA: 0 };
    print("***   Facts count for last hour: " + factsLastHour.count + ", sum of field 'a': " + factsLastHour.sumA);

    // Количество и сумма фактов за последние сутки
    const factsLastDay = stats.factsLastDay && stats.factsLastDay.length > 0 ? stats.factsLastDay[0] : { count: 0, sumA: 0 };
    print("***   Facts count for last day: " + factsLastDay.count + ", sum of field 'a': " + factsLastDay.sumA);

    // Сумма поля 'a' для фактов за последний час с суммой больше 100000
    const sumALastHour = stats.sumALastHour && stats.sumALastHour.length > 0 ? stats.sumALastHour[0].totalSum : 0;
    print("***   Sum of field 'a' for facts in last hour (a > 100000): " + sumALastHour);
  }
  print("");

  const factIndexCount = dbStat.factIndex.find({ "_id.h": hash }).count();
  print("***   Total factIndex with " + hashIndex + " hash : " + factIndexCount);
}


// Создание зон шардирования
function OldCreateShardZones(databaseName = "counters") {
  sh.stopBalancer();
  print("Creating shard zones:");
  var listShards = db.adminCommand({
    listShards: 1
  });
  if (!listShards.ok) {
    print("ERROR: Can not get listShards: " + listShards.errmsg);
    return false;
  }
  const shards = listShards.shards.map(shard => shard._id);
  print("Shards: " + shards.join(", "));
  // Добавление зон шардирования
  shards.forEach(shard => {
    var res = sh.addShardTag(shard, shard);
    if (!res.ok) {
      print("ERROR: Can not add shard <" + shard + "> to tag: " + res.errmsg);
      return false;
    }
    print("Shard <" + shard + "> added to tag: " + shard);
  });
  shards.forEach(shard => {
    var res = sh.addShardToZone(shard, shard);
    if (!res.ok) {
      print("ERROR: Can not add shard <" + shard + "> to zone: " + res.errmsg);
      return false;
    }
    print("Shard <" + shard + "> added to zone: " + shard);
  });

  // Добавление диапазонов ключей
  const ranges = [
    {
      namespace: databaseName + ".facts",
      keys: [
        { i: MinKey },
        { i: "5555555555555555555555555555555555555555" },
        { i: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" },
        { i: MaxKey }
      ]
    },
    {
      namespace: databaseName + ".factIndex",
      keys: [
        { h: MinKey, i: MinKey },
        { h: "5555555555555555555555555555555555555555555555555555555555555555", i: MinKey },
        { h: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", i: MinKey },
        { h: MaxKey, i: MaxKey }
      ]
    },
  ];
  print("*** TAGS ********************************************")
  ranges.forEach(range => {
    // Сначала удалим существующие диапазоны
    print("Removing tag ranges <" + range.namespace + ">");
    // Удаляем старые зоны коллекции
    db.getSiblingDB("config").tags.deleteMany({ "ns": range.namespace });

    // Затем добавляем новые диапазоны
    print("Adding tag ranges <" + range.namespace + ">");
    let count = 0;
    shards.forEach(shard => {
      const res = sh.addTagRange(range.namespace, range.keys[count], range.keys[count + 1], shard);
      if (!res.ok) {
        print("ERROR: Can not add tag range <" + count + "> for shard <" + shard + ">: " + res.errmsg);
        return false;
      }
      print("Tag range <" + count + "> for shard <" + shard + "> added successfully.");
      count++;
    });
  });
  /*
  print("*** ZONES ********************************************")
  ranges.forEach(range => {
      // Затем добавляем новые диапазоны
      print("Adding zone ranges <"+range.namespace+">");
      let count = 0;
      shards.forEach(shard => {
          const res = sh.updateZoneKeyRange(range.namespace, range.keys[count], range.keys[count+1], shard);
          if (!res.ok) {
              print("ERROR: Can not add zone range <"+count+"> for shard <"+shard+">: " + res.errmsg);
              return false;
          }
          print("Zone range <"+count+"> for shard <"+shard+"> added successfully.");
          count++;
      });
  });
  */
  sh.startBalancer();
  return true;
}


/**
 * Удаление диапазона ключей из зоны шардирования
admin = db.getSiblingDB("admin")
admin.runCommand(
   {
      updateZoneKeyRange : "exampledb.collection",
      min : { a : 0 },
      max : { a : 10 },
      zone : null
   }
)

 f = db.facts.stats()
 shards = f.shards

 
 * 
 */
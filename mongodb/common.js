load("base.js");

function ExtractFirstKeyOfIndex(tag) {
  var res = "";
  for (var key in tag) {
    res = key + ": " + JSON.stringify(tag[key]);
    break;
  }
  return res;
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
  /*
  // 
  const DEFAULT_HASH_256_F1 = "3d44a4f94e14b151ddfce48d5b047d1b5b47bbc07ddfb4980296e177453b7119";
  const DEFAULT_HASH_256_F2 = "9fc9fc4ff4620d5c4ffcfe7e7047e854971aa3a2f9a067c85e0bfef624046cd9";
  const DEFAULT_HASH_256_F3 = "5f430702f22fc7c148f715ead5bd1df7b2ccd9ac1b1d54076063965c5153ca34";
  const DEFAULT_HASH_256_F4 = "223d04c3cf66071f7249b22b64bc9d70074a45358451f71dab1a20d786713165";
  const DEFAULT_HASH_256_F5 = "a0fc91f2c3b80325ffd09febfe58e2e9dc5c83cd867ce9ab6cf5facefd6411e8";
  const DEFAULT_HASH_256_F6 = "bb0023bb1d2082be65f3fbe7a95f7926d41da16b19de18ff8832ee9ede9e0027";
  const DEFAULT_HASH_256_F7 = "e64301d6f9fa218cff3f2d9697587bc841f2fc683ce64a6df0a3a66663fb4d8f";
  const DEFAULT_HASH_256_F8 = "5a4a9d00bad8d223817765cc36298677c104fca9164329ccb3b03dcfade26fcd";
  const DEFAULT_HASH_256_F9 = "7df14eec2cba6d33541aabb13eb72645a39cde8c601fd9fce56f1f438cdb86d2";
  const DEFAULT_HASH_256_F10 = "1e1bcae876a8c564faaa5dda18c33f91f2af284c6a9faed855e2a89641d25758";
  const DEFAULT_HASH_256_F11 = "2d08728bb5d69b9f2487835a332c184412cc230d485357fff146cc4775e8586d";
  const DEFAULT_HASH_256_F12 = "ee929a1383c2a9cc5644530214b3a0fabf0803a773446cc3db139918100e39ad";
  const DEFAULT_HASH_256_F13 = "5a9cd1c67f125d6b0c911a49ffedfd4f12fc65a4448b9524f38044634dd36ae8";
  const DEFAULT_HASH_256_F14 = "2699a49f1aabc4b3ec86bd87017819ac9dac8148d52bdee533fd45c4891b71f6";
  const DEFAULT_HASH_256_F15 = "8f36e272e12fa685fa361992c1170f41e5776d5be6a514d1fd5dff68717edb35";
  const DEFAULT_HASH_256_F16 = "7b259c916f68b0bdbc71e3e2aa4651635c42893d83de953679a7f3f694096405";
  const DEFAULT_HASH_256_F17 = "15d69c37a5b5400008b4a572719886daa3bce0aa42bac2e928df49bff7e4b053";
  const DEFAULT_HASH_256_F18 = "a39f0e187873504d85ca4954106de9556d19df6bc84cd12c16caed057f00164f";
  const DEFAULT_HASH_256_F19 = "2c75ada49cb3e918c3631358f681686212ced5f236c8745a4d9dce1efe8c5941";
  const DEFAULT_HASH_256_F20 = "afce5cae1fac23de374c37aee4f265bd3b52080b80cd571d84e7acdf9e409213";
  const DEFAULT_HASH_256_F21 = "cfb3a4d83c2a47af9e2d93ae6e8aef3f5c563d7da9d08bc6784e0375cb35a3e9";
  const DEFAULT_HASH_256_F22 = "49a406912a40c96b6bca1230b43756add95a7b8924fb142fbfe3de163b57ac25";
  const DEFAULT_HASH_256_F23 = "4e7718c5708028249cbfdf1292476a171e32336a01b8b593bcae83ae995fd130";
  const DEFAULT_HASHES_256 = [
    DEFAULT_HASH_256_F1,
    DEFAULT_HASH_256_F2,
    DEFAULT_HASH_256_F3,
    DEFAULT_HASH_256_F4,
    DEFAULT_HASH_256_F5,
    DEFAULT_HASH_256_F6,
    DEFAULT_HASH_256_F7,
    DEFAULT_HASH_256_F8,
    DEFAULT_HASH_256_F9,
    DEFAULT_HASH_256_F10,
    DEFAULT_HASH_256_F11,
    DEFAULT_HASH_256_F12,
    DEFAULT_HASH_256_F13,
    DEFAULT_HASH_256_F14,
    DEFAULT_HASH_256_F15,
    DEFAULT_HASH_256_F16,
    DEFAULT_HASH_256_F17,
    DEFAULT_HASH_256_F18,
    DEFAULT_HASH_256_F19,
    DEFAULT_HASH_256_F20,
    DEFAULT_HASH_256_F21,
    DEFAULT_HASH_256_F22,
    DEFAULT_HASH_256_F23
  ];
  */
  const DEFAULT_HASH_1_F1 = "eb3ca68ffa113f08dda46f50e15655c275a4beda";
  const DEFAULT_HASH_1_F2 = "6698bc28dd22092ce1a0de69a7ac5e22930dd29b";
  const DEFAULT_HASH_1_F3 = "f8e4c4d11305d9df614ba28acbaf03a679664d59";
  const DEFAULT_HASH_1_F4 = "4cd12da049112dcc21f6aa290340c7de93fd8a54";
  const DEFAULT_HASH_1_F5 = "9ff9c6cdbe0fa4bce4f0af04e3c396e1577516a3";
  const DEFAULT_HASH_1_F6 = "1331ea9d3531c54f750ee9928f50db91c1e08380";
  const DEFAULT_HASH_1_F7 = "9b151cd59eca4ebf2f9f038314feb26103f0dc9f";
  const DEFAULT_HASH_1_F8 = "d1e89f9c40b01e977ab1222c225cc7655cdacfa2";
  const DEFAULT_HASH_1_F9 = "70036abf990798e46484382bddaf04eeb07f8781";
  const DEFAULT_HASH_1_F10 = "372707206dc10311b3ea8418fe274a7a5adcd305";
  const DEFAULT_HASH_1_F11 = "f859ad2b22b66f728dd18c36381e6783f9b4be45";
  const DEFAULT_HASH_1_F12 = "f502e3509141ec7b64f30b55df1238de0b9cf617";
  const DEFAULT_HASH_1_F13 = "595027bef2bebb69e2d16aea8597546cfd5f4fb5";
  const DEFAULT_HASH_1_F14 = "a18f7c808bfc8caf076dc6bc9c510335243c837e";
  const DEFAULT_HASH_1_F15 = "3e0ce915b724e2a482a03535ce47296e0855e9b6";
  const DEFAULT_HASH_1_F16 = "e6f4fad9adb0a421d0376e547891e9124f1ea82f";
  const DEFAULT_HASH_1_F17 = "10bd82cf96bcc2d2e6f09866f6c2f53922236d8a";
  const DEFAULT_HASH_1_F18 = "9d89be67a8a82ab4fb71d29e8ffc59ada55996e7";
  const DEFAULT_HASH_1_F19 = "3fc67746b50e269b2f5b860d07a679cb42a2d3ee";
  const DEFAULT_HASH_1_F20 = "12dd93e1abb2340492c7f312ac3fbf1839551394";
  const DEFAULT_HASH_1_F21 = "6177a607258e7e569b1ee7b22a59e02dea83df9f";
  const DEFAULT_HASH_1_F22 = "5042929c9695618c96e24ee1fb0ce2f8066afdbf";
  const DEFAULT_HASH_1_F23 = "9c6120c797f357d1c6719ecd95e8bf6208b9762b";
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
    DEFAULT_HASH_1_F10,
    DEFAULT_HASH_1_F11,
    DEFAULT_HASH_1_F12,
    DEFAULT_HASH_1_F13,
    DEFAULT_HASH_1_F14,
    DEFAULT_HASH_1_F15,
    DEFAULT_HASH_1_F16,
    DEFAULT_HASH_1_F17,
    DEFAULT_HASH_1_F18,
    DEFAULT_HASH_1_F19,
    DEFAULT_HASH_1_F20,
    DEFAULT_HASH_1_F21,
    DEFAULT_HASH_1_F22,
    DEFAULT_HASH_1_F23
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
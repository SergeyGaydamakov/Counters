var COMMON_LOADED = true;
print("Loading common.js");

var SECONDS_IN_MINUTE = NumberInt(60);
var SECONDS_IN_HOUR = NumberInt(60*60);
var SECONDS_IN_DAY = NumberInt(24*60*60);
var MILLISECONDS_IN_MINUTES = SECONDS_IN_MINUTE*1000;

//********************************************************************************************************************
//
// Функции контроля ошибок
//
//********************************************************************************************************************
var SCRIPT_HAS_ERROR = false;

// Очистка признака ошибки
function ClearError() {
    SCRIPT_HAS_ERROR = false;
}

// Функция для проверки результатов выполнения функции
// и фиксация признака ошибки.
function CheckResultError(result) {
    if (!result)
        SCRIPT_HAS_ERROR = true;
    return result;
}

// Убрать эту функцию
function CheckError(msg, connDB) {
/*
    if ((connDB ? connDB : db).getLastError()){
        SCRIPT_HAS_ERROR = true;
        print(msg + ":   " + (connDB ? connDB : db).getLastError());
        return true;
    };
*/	
    return false;
}

function HasError() {
    return SCRIPT_HAS_ERROR;
}


//********************************************************************************************************************
// Функции для телефона
//********************************************************************************************************************

    function PhoneNumberSearch(phone){
        var search = "";
        for(var i=0; i<phone.length; i++){
            if ((phone.charAt(i) >= "0") && (phone.charAt(i) <= "9")) {
                search += phone.charAt(i);
            }
        }
        return search;
    }

    function PhoneNumberLength(phone){
        return PhoneNumberSearch(phone).length;
    }

    function CorrectPhone(phone){
        var l = PhoneNumberLength(phone);
        if (phone.charAt(0) == "8") {
            return phone;
        }
        if (phone.charAt(0) == "+" ) {
            if (l < 10) {
                // Плюса быть не должно
                return phone.slice(1);
            }
            if (l == 10 && (phone.charAt(1) != "7") ) {
                // Забыт 7 и дожен быть плюс
                return "+7"+phone.slice(1);
            }
            if (l == 11 && phone.charAt(1) != "7") {
                // Плюса быть не должно
                return phone.slice(1);
            }
            return phone;
        }
        if ((l == 10) && (phone.charAt(0) != "7")) {
            return "+7"+phone;
        }
        return phone;
    }

//********************************************************************************************************************
// Случайная величина в указанном диапазоне
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

//
function IntToTorrowId( n ) {
	var s = ("111111111111111111111111"+n.valueOf().toString()).slice(n.valueOf().toString().length);
	return {
		s: NumberInt( n % 10 ),
		t: NumberInt(1),
		o: ObjectId(s)
	};
}

//
// Функции кодирования и декодирования base64.
// Исходники взяты:
// https://gist.github.com/AndreasMadsen/2693051
// base64encode('Hello, world!'); // 'SGVsbG8sIHdvcmxkIQ=='
// base64decode('SGVsbG8sIHdvcmxkIQ=='); // 'Hello, world!'
var base64EncodeChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
var base64DecodeChars = new Array(
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
    -1,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
    -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1);

function base64encode(str) {
    var out, i, len;
    var c1, c2, c3;

    len = str.length;
    i = 0;
    out = "";
    while(i < len) {
  c1 = str.charCodeAt(i++) & 0xff;
  if(i == len)
  {
      out += base64EncodeChars.charAt(c1 >> 2);
      out += base64EncodeChars.charAt((c1 & 0x3) << 4);
      out += "==";
      break;
  }
  c2 = str.charCodeAt(i++);
  if(i == len)
  {
      out += base64EncodeChars.charAt(c1 >> 2);
      out += base64EncodeChars.charAt(((c1 & 0x3)<< 4) | ((c2 & 0xF0) >> 4));
      out += base64EncodeChars.charAt((c2 & 0xF) << 2);
      out += "=";
      break;
  }
  c3 = str.charCodeAt(i++);
  out += base64EncodeChars.charAt(c1 >> 2);
  out += base64EncodeChars.charAt(((c1 & 0x3)<< 4) | ((c2 & 0xF0) >> 4));
  out += base64EncodeChars.charAt(((c2 & 0xF) << 2) | ((c3 & 0xC0) >>6));
  out += base64EncodeChars.charAt(c3 & 0x3F);
    }
    return out;
}

function base64decode(str) {
    var c1, c2, c3, c4;
    var i, len, out;

    len = str.length;
    i = 0;
    out = "";
    while(i < len) {
  /* c1 */
  do {
      c1 = base64DecodeChars[str.charCodeAt(i++) & 0xff];
  } while(i < len && c1 == -1);
  if(c1 == -1)
      break;

  /* c2 */
  do {
      c2 = base64DecodeChars[str.charCodeAt(i++) & 0xff];
  } while(i < len && c2 == -1);
  if(c2 == -1)
      break;

  out += String.fromCharCode((c1 << 2) | ((c2 & 0x30) >> 4));

  /* c3 */
  do {
      c3 = str.charCodeAt(i++) & 0xff;
      if(c3 == 61)
    return out;
      c3 = base64DecodeChars[c3];
  } while(i < len && c3 == -1);
  if(c3 == -1)
      break;

  out += String.fromCharCode(((c2 & 0XF) << 4) | ((c3 & 0x3C) >> 2));

  /* c4 */
  do {
      c4 = str.charCodeAt(i++) & 0xff;
      if(c4 == 61)
    return out;
      c4 = base64DecodeChars[c4];
  } while(i < len && c4 == -1);
  if(c4 == -1)
      break;
  out += String.fromCharCode(((c3 & 0x03) << 6) | c4);
    }
    return out;
}

//
//
//

function base64ToHex(str) {
// const raw = atob(str);
  const raw = base64decode(str);
  let result = '';
  for (let i = 0; i < raw.length; i++) {
    const hex = raw.charCodeAt(i).toString(16);
    result += (hex.length === 2 ? hex : '0' + hex);
  }
  return result.toLowerCase();
}

function hexToBase64(hexstring) {
    return base64encode(hexstring.match(/\w{2}/g).map(function(a) {
        return String.fromCharCode(parseInt(a, 16));
    }).join(""));
}

function TorrowIdToBase64(torrowId){
	if (!torrowId){
		return null;
	}
	var objectTypeString = GetObjectTypeString( torrowId.t );
	if (!objectTypeString){
		return null;
	}
	return {
		"shard":torrowId.s,
		"objectType":objectTypeString,
		"objectId":hexToBase64( torrowId.o.toHexString() )
	};
}

function Base64ToTorrowId(base64TorrowId){
	if (!base64TorrowId){
		return null;
	}
	var objectType = GetObjectTypeFromString( base64TorrowId.objectType );
	if (!objectType){
		return null;
	}
	return {
		"s":NumberInt(base64TorrowId.shard),
		"t":objectType,
		"o":base64ToHex( base64TorrowId.objectId )
	};
}

function PackTorrowId( tid ){
  if (!tid){
	  return null;
  }
  if (typeof( tid ) == "string"){
	  return tid;
  }
  const radix = 16;
  const objectTypeLength = 4;
  const objectType = tid.t.toString(radix).padStart(objectTypeLength, "0");
  const shard = tid.s.toString(radix);
  return (shard+objectType+tid.o.toHexString()).replace(/^0+/, "");
}

function UnpackTorrowId( stringTid ){
    if (!stringTid){
	    return null;
	}
    // 103ea999900000000000000000108
    var s = Number.parseInt(stringTid.slice(0,-28),16);
    var t = Number.parseInt(stringTid.slice(-28,-24),16);
    var o = ObjectId(stringTid.slice(-24));
    return {"s": NumberInt(s), "t": NumberInt(t), "o": o};
}

function ConvertToTorrowId( anyTid ){
	if (!anyTid) {
		return null;
	}
    var tid = null;
    if (typeof( anyTid ) == "string"){
        tid = UnpackTorrowId( anyTid );
    } else if (typeof( anyTid ) == "object") {
        tid = anyTid;
    } else {
        print( "ConvertToTorrowId: unknown type anyTid " + anyTid);
		throw "ConvertToTorrowId: unknown type anyTid " + anyTid;
    }
    return tid;
}

function ExtractObjectId( stringTid ){
    return UnpackTorrowId( stringTid );
}

function GetTorrowIdObject( tid ){
    return {"s": NumberInt(tid.s), "t": NumberInt(tid.t), "o": tid.o};
}

function TorrowIdToString( torrowIdentifier ){
	if (!torrowIdentifier) {
		return "null";
	}
	var tid = ConvertToTorrowId( torrowIdentifier );
	return "{s: NumberInt("+tid.s+"), t: NumberInt("+tid.t+"), o: ObjectId('"+tid.o.toHexString()+"')}";
}

function TorrowIdToFindCondition( tid, fieldName, unionJSON ){
    return FindConditionByTid( tid, fieldName, unionJSON );
}

function FindConditionByTid( anyTid, fieldName, unionJSON ){
    var tid = ConvertToTorrowId( anyTid );
    if (!tid) {
        return null;
    }
    var name = "tid";
    if (fieldName) {
        name = fieldName;
    }
    var res = {};
    res[name+".s"] = NumberInt(tid.s);
    res[name+".t"] = NumberInt(tid.t);
    res[name+".o"] = tid.o;
    if (!unionJSON) {
        return res;
    }
    return MergeJSON(res, unionJSON);
}

/*
function TorrowIdToFindCondition_Old( tid, unionJSON ){
    var res = {
        "i.s": NumberInt(tid.s),
        "i.t": NumberInt(tid.t),
        "i.o": tid.o
    };
    if (!unionJSON) {
        return res;
    }
    return MergeJSON(res, unionJSON);
}
*/

//
function CollectionExists( collName, connDB ){
    return (connDB ? connDB : db).getCollectionNames().some(c => {return IsEqual(c, collName);});
}

function JSON_NumberInt( j, excludeNames, path ){
	if (j == null) {
		return null;
	}
	if ((typeof(j) == "object") && (j.constructor.name == "Array")){
		var result = [];
		for(var key in j){
			result.push( JSON_NumberInt( j[key], excludeNames, path) );
		}
		return result;
	} else if ((typeof(j) == "object") && (j.constructor.name == "Object")){
		var result = {};
		for(var key in j){
			if (j.hasOwnProperty(key)) {
				var fullKey = (path ? path+(Array.isArray(j) ? "" : "."+key) : (Array.isArray(j) ? "" : key));
				var skipKey = excludeNames && excludeNames.includes(fullKey);
				if (!skipKey) {
					result[key] = JSON_NumberInt( j[key], excludeNames, fullKey);
				} else {
					result[key] = j[key];
				}
			}
		}
		return result;
	} else if (typeof(j)== "number"){
		if (Number.isInteger(j) == true) {
			return Int32(j.toString());
		} else {
			return j;
		}
	} 
	return j;
}

// JSON_NumberDouble(o, ["pp.pr","pp.sp","pp.tp"])
function JSON_NumberDouble( j, includeNames, path ){
	if (j == null) {
		return null;
	}
	if ((typeof(j) == "object") && (j.constructor.name == "Array")){
		var result = [];
		for(var key in j){
			result.push( JSON_NumberDouble( j[key], includeNames, path) );
		}
		return result;
	} else if ((typeof(j) == "object") && (j.constructor.name == "Object")){
		var result = {};
		for(var key in j){
			if (j.hasOwnProperty(key)) {
				var fullKey = (path ? path+(Array.isArray(j) ? "" : "."+key) : (Array.isArray(j) ? "" : key));
				result[key] = JSON_NumberDouble( j[key], includeNames, fullKey);
			}
		}
		return result;
	} else if ((typeof(j) == "object") && (j.constructor.name == "Double")){
        var convertKey = includeNames && includeNames.includes(path);
        if (convertKey) {
			return Double(j.toString());
		}
	} else if ((typeof(j) == "object") && (j.constructor.name == "Decimal128")){
        var convertKey = includeNames && includeNames.includes(path);
        if (convertKey) {
			return Double(j.toString());
		}
	} else if ((typeof(j) == "object") && (j.constructor.name == "Int32")){
        var convertKey = includeNames && includeNames.includes(path);
        if (convertKey) {
			return Double(j.toString());
		}
	} else if (typeof(j)== "number"){
        var convertKey = includeNames && includeNames.includes(path);
        if (convertKey) {
			return Double(j.toString());
		}
	} 
	return j;
}

// JSON_ObjectId([{tid: "668002d73beab4e763209e88"}], ["tid"])
function JSON_ObjectId( j, includeNames, path ){
	if (j == null) {
		return null;
	}
	if ((typeof(j) == "object") && (j.constructor.name == "Array")){
		var result = [];
		for(var key in j){
			result.push( JSON_ObjectId( j[key], includeNames, path) );
		}
		return result;
	} else if ((typeof(j) == "object") && (j.constructor.name == "Object")){
		var result = {};
		for(var key in j){
			if (j.hasOwnProperty(key)) {
				var fullKey = (path ? path+(Array.isArray(j) ? "" : "."+key) : (Array.isArray(j) ? "" : key));
				result[key] = JSON_ObjectId( j[key], includeNames, fullKey);
			}
		}
		return result;
	} else if (typeof(j)== "string"){
        var convertKey = includeNames && includeNames.includes(path);
        if (convertKey) {
			return ObjectId(j.toString());
		}
	} 
	return j;
}


// Превращаем вложенный JSON в плоский
function PlainJSON( sourceJSON, prefix ){
	if (sourceJSON == null) {
		return null;
	}
	var res = {};
    for(var key in sourceJSON) {
        if (sourceJSON.hasOwnProperty(key)) {
			if ((typeof(sourceJSON[key]) == "object") && (sourceJSON[key].constructor.name == "Object") && Object.keys(sourceJSON[key]).length) {
				var innerJSON = PlainJSON( sourceJSON[key], key );
				for (var innerKey in innerJSON){
					res[innerKey] = innerJSON[innerKey];
				}
			} else {
				res[(prefix ? prefix + "." : "")+key] = sourceJSON[key];
			}
		}
    }
	return res;
}



function JSON_NumberInt_Test( j, excludeNames, path ){
	if (!j) {
		return null;
	}
	print(path + " = " + JSON.stringify(j));
	if ((typeof(j) == "object") && (j.constructor.name == "Array")){
		var result = [];
		for(var key in j){
			result.push( JSON_NumberInt( j[key], excludeNames, path) );
		}
		return result;
	}
	if (typeof(j) == "string"){
		return j;
	}
	var result = {};
	var hasProperty = false;
    for(var key in j){
		if (j.hasOwnProperty(key)) {
			hasProperty= true;
			var fullKey = (path ? path+(Array.isArray(j) ? "" : "."+key) : (Array.isArray(j) ? "" : key));
			var skipKey = excludeNames && excludeNames.includes(fullKey);
			if (!skipKey) {
				if (j[key] == null) {
					result[key] = null;
				} else if (typeof(j[key])== "Number"){
					if (Number.isInteger(j[key]) == true) {
						result[key] = Int32(j[key].toString());
					} else {
						result[key] = j[key];
					}
				} else if (typeof(j[key]) == "object"){
						if (j[key].constructor.name == "Int32") {
							result[key] = j[key];
						} else if (j[key].constructor.name == "Long") {
							result[key] = j[key];
//							result[key] = Int32(j[key].toString());
						} else if (j[key].constructor.name == "Double") {
							result[key] = j[key];
//							result[key] = Int32(j[key].toString());
						} else if (j[key].constructor.name == "Decimal128") {
							result[key] = j[key];
//							result[key] = Int32(j[key].toString());
						} else if (j[key].constructor.name == "Object") {
							result[key] = JSON_NumberInt( j[key], excludeNames, fullKey);
						} else if (j[key].constructor.name == "Array") {
							result[key] = JSON_NumberInt( j[key], excludeNames, fullKey);
						} else {
							result[key] = j[key];
						}
				} else {
					print(path + " = " + typeof(j[key]) + " " + JSON.stringify(j));
					result[key] = j[key];
				}
			} else {
				result[key] = j[key];
			}
		}
    }
	if (!hasProperty) {
		return j;
	}
    return result;
}

function JSON_NumberInt_Old( j, excludeNames, path ){
    for(var key in j){
		var fullKey = (path ? path+(Array.isArray(j) ? "" : "."+key) : (Array.isArray(j) ? "" : key));
		var skipKey = excludeNames && excludeNames.includes(fullKey);
		if (!skipKey) {
			if (typeof(j[key]) == "object"){
					j[key] = JSON_NumberInt( j[key], excludeNames, fullKey);
			} else if (Number.isInteger(j[key])){
				j[key] = Int32(j[key].toString());
			}
		}
    }
    return j;
}


function JSON_ReplaceString( j, subString, newSubString ){
	if (j == null){
		return null;
	}
	if ((typeof(j) == "object") && (j.constructor.name == "Array")){
		var result = [];
		for(var key in j){
			result.push( JSON_ReplaceString( j[key], subString, newSubString ) );
		}
		return result;
	} else if ((typeof(j) == "object") && (j.constructor.name == "Object")){
		var result = {};
		for(var key in j){
			if (j.hasOwnProperty(key)) {
				result[key] = JSON_ReplaceString( j[key], subString, newSubString );
			}
		}
		return result;
	} else if (typeof(j[key])=="string"){
		return j.replace(subString, newSubString);
	}
    return j;
}


function JSON_ReplaceString_Test( j, subString, newSubString ){
	if (!j){
		return null;
	}
	if ((typeof(j) == "object") && (j.constructor.name == "Array")){
		var result = [];
		for(var key in j){
			result.push( JSON_ReplaceString( j[key], subString, newSubString ) );
		}
		return result;
	} else if ((typeof(j) == "object") && (j.constructor.name == "Object")){
	}
	var result = {};
	var hasProperty = false;
    for(var key in j){
		if (j.hasOwnProperty(key)) {
			hasProperty= true;
			if (j[key] == null) {
				result[key] = null;
			} else if (typeof(j[key])=="string"){
				result[key] = j[key].replace(subString, newSubString);
			} else if (typeof(j[key]) == "object"){
				if (j[key].constructor.name == "Object") {
					result[key] = JSON_ReplaceString( j[key], subString, newSubString );
				} else if (j[key].constructor.name == "Array") {
					result[key] = JSON_ReplaceString( j[key], subString, newSubString );
				} else {
					result[key] = j[key];
				}
			} else {
				result[key] = j[key];
			}
		}
    }
	if (!hasProperty) {
		return j;
	}
    return result;
}

function JSON_ReplaceString_Old( j, subString, newSubString ){
    for(var key in j){
        if (typeof(j[key])=="string"){
            j[key] = j[key].replace(subString, newSubString);
        }
        if (typeof(j[key]) == "object"){
            j[key] = JSON_ReplaceString( j[key], subString, newSubString );
        }
    }
    return j;
}


//
function ExtractProcName( func ){
    var procName = func.toString().match(/^function\s*(\w*)/i);
    if (!procName){
        return "";
    }
    if (!procName[1]){
        return "";
    }
    return procName[1];
}

//*******************************************************************************************
//
// Вспомогательные функции для пакетной обработки
// https://www.percona.com/blog/2018/04/05/managing-mongodb-bulk-deletes-inserts/
// Так как выполняется репликация, то пакеты обновлений не должны быть слишком частые и длинные
//
//*******************************************************************************************

// db.bar.deleteBulk({type:"archive"},1000,20);
/*
var USE_BULK_FUNCTIONS = false;
if (USE_BULK_FUNCTIONS){
DBCollection.prototype.deleteBulk = function( query, batchSize, pauseMS){
    var batchBucket = new Array();
    var collection = this;
    var currentCount = 0;
    var firstCycle = true;
    var totalCount = collection.find(query,{_id: 1}).count();
    collection.find(query, {_id: 1}).noCursorTimeout().forEach(function(doc){
        batchBucket.push(doc._id);
        if ( batchBucket.length >= batchSize) {
            collection.deleteOne({_id : { "$in" : batchBucket}});
            currentCount += batchBucket.length;
            batchBucket = [];
            if (!firstCycle) {
                print("         removed in " + collection.getFullName() + ": " + currentCount + " from "+totalCount);
                sleep (pauseMS);
            }
            firstCycle = false;
        }
    });
    if (batchBucket.length) {
        collection.deleteOne({_id : { "$in" : batchBucket}});
        currentCount += batchBucket.length;
    }
    print("   total removed in " + collection.getFullName() + ": " +currentCount+"...");
    return currentCount;
}
}
*/
function deleteBulk( collection, query, batchSize, pauseMS){
    if (!collection) {
        print("ERROR Bad parameters procedure deleteBulk: collection is null.");
        return 0;
    }
    var batchBucket = new Array();
    var currentCount = 0;
    var firstCycle = true;
    var totalCount = collection.find(query,{_id: 1}).count();
    collection.find(query, {_id: 1}).noCursorTimeout().forEach(function(doc){
        batchBucket.push(doc._id);
        if ( batchBucket.length >= batchSize) {
            collection.deleteOne({_id : { "$in" : batchBucket}});
            currentCount += batchBucket.length;
            batchBucket = [];
            if (!firstCycle) {
                print("         removed in " + collection.getFullName() + ": " + currentCount + " from "+totalCount);
                sleep (pauseMS);
            }
            firstCycle = false;
        }
    });
    if (batchBucket.length) {
        collection.deleteOne({_id : { "$in" : batchBucket}});
        currentCount += batchBucket.length;
    }
    print("   total removed in " + collection.getFullName() + ": " +currentCount+"...");
    return currentCount;
}


// bucket - массив _id
// changeObject - обновляемый объект
function BulkUpdateBucket( collection, bucket, changeObject ) {
    if (!collection) {
        print("ERROR Bad parameters procedure BulkUpdateBucket: collection is null.");
        return 0;
    }
    if (!bucket.length) {
        return 0;
    }
    var bulk = collection.initializeUnorderedBulkOp();
    bucket.forEach(function(id){
        bulk.find({_id: id}).updateOne(changeObject);
    })
    var res = bulk.execute();
    if (res.writeErrors && res.writeErrors.length || res.writeConcernErrors && res.writeConcernErrors.length) {
        print("ERROR bulk update:");
        printjson( query );
        printjson( changeObject );
        printjson( res );
    }
    return bucket.length;
}

/*
if (USE_BULK_FUNCTIONS){
DBCollection.prototype.updateBulk = function( query, changeObject, batchSize, pauseMS){
    var batchBucket = new Array();
    var totalToProcess = null;
//    if (totalToProcess < batchSize){ batchSize = totalToProcess; }
    currentCount = 0;
    var firstCycle = true;
    var collection = this;
    if (batchSize > 500) {
        // Предполагаем, что очень много данных, поэтому посчитаем сколько
        totalToProcess = collection.find(query,{_id:1}).count();
    }
    collection.find(query, {_id: 1}).noCursorTimeout().forEach(function(doc){
        batchBucket.push(doc._id);
        if ( batchBucket.length >= batchSize){
            currentCount += BulkUpdateBucket(collection, batchBucket, changeObject);
            batchBucket = [];
            if (!firstCycle) {
                print("         updated in "+collection.getFullName()+": "+currentCount+(totalToProcess ? " from "+totalToProcess+" " : "") + "...");
                sleep (pauseMS);
            }
            firstCycle = false;
        }
    });
    currentCount += BulkUpdateBucket(collection, batchBucket, changeObject);
    if (currentCount && !firstCycle) {
        print("   total updated in "+collection.getFullName()+": "+currentCount+"...");
    }
    return currentCount;
}
}
*/

function updateBulk( collection, query, changeObject, batchSize, pauseMS){
    if (!collection) {
        print("ERROR Bad parameters procedure updateBulk: collection is null.");
        return 0;
    }
    var batchBucket = new Array();
    var totalToProcess = null;
//    if (totalToProcess < batchSize){ batchSize = totalToProcess; }
    currentCount = 0;
    var firstCycle = true;
    if (batchSize > 500) {
        // Предполагаем, что очень много данных, поэтому посчитаем сколько
        totalToProcess = collection.find(query,{_id:1}).count();
    }
    collection.find(query, {_id: 1}).noCursorTimeout().forEach(function(doc){
        batchBucket.push(doc._id);
        if ( batchBucket.length >= batchSize){
            currentCount += BulkUpdateBucket(collection, batchBucket, changeObject);
            batchBucket = [];
            if (!firstCycle) {
                print("         updated in "+collection.getFullName()+": "+currentCount+(totalToProcess ? " from "+totalToProcess+" " : "") + "...");
                sleep (pauseMS);
            }
            firstCycle = false;
        }
    });
    currentCount += BulkUpdateBucket(collection, batchBucket, changeObject);
    if (currentCount && !firstCycle) {
        print("   total updated in "+collection.getFullName()+": "+currentCount+"...");
    }
    return currentCount;
}

function GetKeyProjection(collFullName){
	var shardKey = GetShardKeyForCollection( collFullName );
	if (shardKey) {
		return MergeJSON( shardKey, {_id: 0} );
	} else {
		return {_id: 1}
	}
}

// Получение шардированного ключа коллекции.
// Если коллекция отсутсвует или не шардирована, то возвращается null
var _CashShardKeys = [];		// [{Key: "", Value: JSON}]
function GetShardKeyForCollection( collFullName ){
	var cash = _CashShardKeys.filter(i => IsEqual(i.key, collFullName))[0];
	if (cash) {
		return cash.value;
	}
	var config = db.getSiblingDB("config");
	if (!config) {
		throw "Can not get access to 'config' database to read shard key info.";
	}
	var res = null;
	var collInfo = config.collections.findOne({_id: collFullName});
	if (!collInfo || !collInfo.key){
		res = null;
	} else {
		res = collInfo.key;
	} 
	// Сохранение в кеш
	_CashShardKeys.push({key: collFullName, value: res});
	return res;
}

// CheckInfoToShardKey( { "i.s" : 1, "i.o" : 1, "u.o" : 1 }, {"i.s" : 7004,"i.o" : ObjectId("6131bf9177254fa2bb6e6a67"),"u.o" : ObjectId("e90048adafcc3c0001d327d3")} )
function CheckInfoToShardKey( checkedInfo, shardKey ){
	var successCheck = true;
    for(var key in shardKey) {
        if(!checkedInfo.hasOwnProperty(key)) {
			successCheck = false;
		} else if (checkedInfo[key] === undefined) {
			successCheck = false;
		} 
    }
	return successCheck;
}

// shardBucket - массив уникальных ключей для удаления документов
// {_id: _id } или по шардированному ключу
function ShardBulkRemoveBucket( collection, shardBucket ) {
    if (!collection) {
        print("ERROR Bad parameters procedure ShardBulkRemoveBucket: collection is null.");
        return 0;
    }
    if (!shardBucket.length) {
        return 0;
    }
	// Проверяем, что структура shardBucket соответствует шардированному ключу
	var shardKey = GetShardKeyForCollection(collection.getFullName());
	if (!CheckInfoToShardKey(PlainJSON(shardBucket[0]), shardKey)) {
		throw "ShardBucket has wrong fields "+tojson(shardBucket[0])+". Expected shard key structure: "+tojson(shardKey);
	}
	
    var bulk = collection.initializeUnorderedBulkOp();
    shardBucket.forEach(function(key){
        bulk.find(PlainJSON(key)).removeOne();
    })
    var res = bulk.execute();
    if (res.writeErrors && res.writeErrors.length || res.writeConcernErrors && res.writeConcernErrors.length) {
        print("ERROR bulk remove:");
        printjson( query );
        printjson( res );
    }
    return shardBucket.length;
}


// db.bar.deleteBulk({type:"archive"},1000,20); 
/* Не работает под Mongosh.exe   
if (USE_BULK_FUNCTIONS){
DBCollection.prototype.deleteShardBulk = function( query, batchSize, pauseMS){
    var batchBucket = new Array();
    var collection = this;
    var currentCount = 0;
    var firstCycle = true;
	var totalToProcess = null;
    if (batchSize > 500) {
        // Предполагаем, что очень много данных, поэтому посчитаем сколько
        totalToProcess = collection.find(query,{_id:1}).count();
    }

	var keyProjection = GetKeyProjection(collection.getFullName());
    collection.find(query, keyProjection ).noCursorTimeout().forEach(function(doc){
        batchBucket.push(doc);
        if ( batchBucket.length >= batchSize) {
            currentCount += ShardBulkRemoveBucket(collection, batchBucket);
            batchBucket = [];
            if (!firstCycle) {
                print("         removed in " + collection.getFullName() + ": " + currentCount + (totalToProcess ? " from "+totalToProcess+" " : "") + "...");
                sleep (pauseMS);
            }
            firstCycle = false;
        }
    });
    if (batchBucket.length) {
        currentCount += ShardBulkRemoveBucket(collection, batchBucket);
    }
    print("   total removed in " + collection.getFullName() + ": " +currentCount+"...");
    return currentCount;
}
}
*/

function deleteShardBulk( collection, query, batchSize, pauseMS){
    if (!collection) {
        print("ERROR Bad parameters procedure deleteShardBulk: collection is null.");
        return 0;
    }
    var batchBucket = new Array();
    var currentCount = 0;
    var firstCycle = true;
	var totalToProcess = null;
    if (batchSize > 500) {
        // Предполагаем, что очень много данных, поэтому посчитаем сколько
        totalToProcess = collection.find(query,{_id:1}).count();
    }

	var keyProjection = GetKeyProjection(collection.getFullName());
    collection.find(query, keyProjection ).noCursorTimeout().forEach(function(doc){
        batchBucket.push(doc);
        if ( batchBucket.length >= batchSize) {
            currentCount += ShardBulkRemoveBucket(collection, batchBucket);
            batchBucket = [];
            if (!firstCycle) {
                print("         removed in " + collection.getFullName() + ": " + currentCount + (totalToProcess ? " from "+totalToProcess+" " : "") + "...");
                sleep (pauseMS);
            }
            firstCycle = false;
        }
    });
    if (batchBucket.length) {
        currentCount += ShardBulkRemoveBucket(collection, batchBucket);
    }
    print("   total removed in " + collection.getFullName() + ": " +currentCount+"...");
    return currentCount;
}


// shardBucket - массив уникальных ключей для удаления документов
// {_id: _id } или по шардированному ключу
// changeObject - обновляемый объект
function ShardBulkUpdateBucket( collection, shardBucket, changeObject ) {
    if (!collection) {
        print("ERROR Bad parameters procedure ShardBulkUpdateBucket: collection is null.");
        return 0;
    }
    if (!shardBucket.length) {
        return 0;
    }
	// Проверяем, что структура shardBucket соответствует шардированному ключу
	var shardKey = GetShardKeyForCollection(collection.getFullName());
	if (!CheckInfoToShardKey(PlainJSON(shardBucket[0]), shardKey)) {
		print("PlainJSON(shardBucket[0])");
		printjson(PlainJSON(shardBucket[0]));
		print("shardKey: ");
		printjson(shardKey);
		throw "ShardBucket has wrong fields "+tojson(shardBucket[0])+". Expected shard key structure: "+tojson(shardKey);
	}
	
    var bulk = collection.initializeUnorderedBulkOp();
    shardBucket.forEach(function(key){
        bulk.find(PlainJSON(key)).updateOne(changeObject);
//        bulk.find(key).updateOne(changeObject);
    })
    var res = bulk.execute();
    if (res.writeErrors && res.writeErrors.length || res.writeConcernErrors && res.writeConcernErrors.length) {
        print("ERROR bulk update:");
        printjson( query );
        printjson( changeObject );
        printjson( res );
    }
    return shardBucket.length;
}

/*
if (USE_BULK_FUNCTIONS){
DBCollection.prototype.updateShardBulk = function( query, changeObject, batchSize, pauseMS){
    var batchBucket = new Array();
    var totalToProcess = null;
//    if (totalToProcess < batchSize){ batchSize = totalToProcess; }
    currentCount = 0;
    var firstCycle = true;
    var collection = this;

	var keyProjection = GetKeyProjection(collection.getFullName());
	
    if (batchSize > 500) {
        // Предполагаем, что очень много данных, поэтому посчитаем сколько
        totalToProcess = collection.find(query,{_id:1}).count();
    }
	// Получаем шардированный ключ для каждого элемента
    collection.find(query, keyProjection ).noCursorTimeout().forEach(function(doc){
        batchBucket.push( doc );
        if ( batchBucket.length >= batchSize){
            currentCount += ShardBulkUpdateBucket(collection, batchBucket, changeObject);
            batchBucket = [];
            if (!firstCycle) {
                print("         updated in "+collection.getFullName()+": "+currentCount+(totalToProcess ? " from "+totalToProcess+" " : "") + "...");
                sleep (pauseMS);
            }
            firstCycle = false;
        }
    });
    currentCount += ShardBulkUpdateBucket(collection, batchBucket, changeObject);
    if (currentCount && !firstCycle) {
        print("   total updated in "+collection.getFullName()+": "+currentCount+"...");
    }
    return currentCount;
}
}
*/

function updateShardBulk( collection, query, changeObject, batchSize, pauseMS){
    if (!collection) {
        print("ERROR Bad parameters procedure updateShardBulk: collection is null.");
        return 0;
    }
    var batchBucket = new Array();
    var totalToProcess = null;
//    if (totalToProcess < batchSize){ batchSize = totalToProcess; }
    currentCount = 0;
    var firstCycle = true;

	var keyProjection = GetKeyProjection(collection.getFullName());
	
    if (batchSize > 500) {
        // Предполагаем, что очень много данных, поэтому посчитаем сколько
        totalToProcess = collection.find(query,{_id:1}).count();
    }
	// Получаем шардированный ключ для каждого элемента
    collection.find(query, keyProjection ).noCursorTimeout().forEach(function(doc){
        batchBucket.push( doc );
        if ( batchBucket.length >= batchSize){
            currentCount += ShardBulkUpdateBucket(collection, batchBucket, changeObject);
            batchBucket = [];
            if (!firstCycle) {
                print("         updated in "+collection.getFullName()+": "+currentCount+(totalToProcess ? " from "+totalToProcess+" " : "") + "...");
                sleep (pauseMS);
            }
            firstCycle = false;
        }
    });
    currentCount += ShardBulkUpdateBucket(collection, batchBucket, changeObject);
    if (currentCount && !firstCycle) {
        print("   total updated in "+collection.getFullName()+": "+currentCount+"...");
    }
    return currentCount;
}


//*******************************************************************************


// Нормализация JSON падежей
var DICTIONARY = {
    CASE_NOMINATIVE : "nom",
    CASE_GENITIVE : "gen",
    CASE_DATIVE : "dat",
    CASE_ACCUSATIVE : "acc",
    CASE_INSTRUMENTAL : "ins",
    CASE_PREPOSITIONAL : "pre",
    LANGUAGE_RUSSIAN : "ru",
    LANGUAGE_ENGLISH : "en"
};

function NormalJSONCases( cases ){
    if (!cases) 
        return null;
    var newCases = {};
    for(var key in cases) {
        for (var d in DICTIONARY){
            if (IsEqual(key, d)) {
                newCases[DICTIONARY[d]] = ( !IsEqual(typeof(cases[key]),"object") ? cases[key] : NormalJSONCases( cases[key] ) );
            }
        };
    };
    return newCases;
};


//*******************************************************************
//
// Функции тестирования
//
//*******************************************************************
var TESTHELPER_IT_WAS_ERROR = false;

function TestHelper_ClearError() {
    TESTHELPER_IT_WAS_ERROR = false;
}

function TestHelper_ItWasError() {
    return TESTHELPER_IT_WAS_ERROR;
}

function TestHelper_Assert( condition, message ) {
    if (!condition) {
        print( "TEST ERROR: " + message );
        TESTHELPER_IT_WAS_ERROR = true;
    }
    return condition;
}

Date.prototype.addSeconds = function(seconds) {
  this.setSeconds(this.getSeconds() + seconds);
  return this;
};

Date.prototype.addMinutes = function(minutes) {
  this.setMinutes(this.getMinutes() + minutes);
  return this;
};

Date.prototype.addHours = function(hours) {
  this.setHours(this.getHours() + hours);
  return this;
};

Date.prototype.addDays = function(days) {
  this.setDate(this.getDate() + days);
  return this;
};

Date.prototype.addWeeks = function(weeks) {
  this.addDays(weeks*7);
  return this;
};

Date.prototype.addMonths = function (months) {
  var dt = this.getDate();
  this.setMonth(this.getMonth() + months);
  var currDt = this.getDate();
  if (dt !== currDt) {  
    this.addDays(-currDt);
  }
  return this;
};

Date.prototype.addYears = function(years) {
  var dt = this.getDate();
  this.setFullYear(this.getFullYear() + years);
  var currDt = this.getDate();
  if (dt !== currDt) {  
    this.addDays(-currDt);
  }
  return this;
};

// Функция, которая выдает дату со смещением в днях, часах и минутах
function GetDate(days, hours, minutes, seconds, offset) {
    var d = new Date();
    d.setUTCMilliseconds(0);
    if (offset) {
        d.setUTCHours(d.getUTCHours()+offset);
    }
    if (days) {
        d.setUTCDate(d.getUTCDate()+days);
    }
    if (hours) {
        d.setUTCHours(d.getUTCHours()+hours);
    }
    if (minutes) {
        d.setUTCMinutes(d.getUTCMinutes()+minutes);
    }
    if (seconds) {
        d.setUTCSeconds(d.getUTCSeconds()+seconds);
    }
    // Обнуляем null значения
    if (hours === null) {
        d.setUTCHours(0);
    }
    if (minutes === null) {
        d.setUTCMinutes(0);
    }
    if (seconds === null) {
        d.setUTCSeconds(0);
    }
    return d;
}


function MonthName( num ) {
    if (!num) {
        return null;
    }
    var arr=[
       'Январь',
       'Февраль',
       'Март',
       'Апрель',
       'Май',
       'Июнь',
       'Июль',
       'Август',
       'Сентябрь',
       'Октябрь',
       'Ноябрь',
       'Декабрь',
    ];
    return arr[num];
}

// Функция разбиения строки на массив отдельных слов без пробелов и знаков препинания.
function ExtractWords( phrase ) {
    var splitPhrase = phrase.toLowerCase().split(/[\s,.]/);
    var res = [];
    splitPhrase.forEach( function( word ) {
        if (word.length > 1) {
            res.push( word );
            var splitWord = word.split(/[\/-]/);
            if (splitWord.length > 1) {
                splitWord.forEach( function(subword) {
                    if (word.length > 1) {
                        res.push( subword );
                    }
                });
            }
        }
    });
    return res;
}

// Функция загрузки данных из файла в кодировке UTF-8
function LoadFile( fileName, print_prefix) {
    var tab = print_prefix;
    if (!tab)
        tab = "";
    var sMsg = null;
    try {
        sMsg = require(fileName);
        print( tab + "File <" + fileName + "> was read.");
    } catch(e) {
        print( "ERROR Error read file <" + fileName + ">: "+ e.message);
        return null;
    }
    return sMsg;
}

// Функция загрузки JSON из файла в кодировке UTF-8
function LoadJSONFile( fileName, print_prefix) {
/*	
    var sMsg = LoadFile(fileName, print_prefix);
	if (!sMsg){
		return null;
	}
*/	
    var jsonMsg = null;
    try {
		jsonMsg = require(fileName);
        //jsonMsg = JSON.parse( sMsg );
    } catch(e) {
        print("ERROR Error parse to JSON file <" + fileName + ">: "+ e.message);
        return null;
    }
    return jsonMsg;
}

function MergeJSON(defaultObj, overrideObj) {
    var result = {};
    for(var key in defaultObj) {
        if(defaultObj.hasOwnProperty(key)) {
            result[key]=defaultObj[key];
        }
    }
    for(var key in overrideObj) {
        if(overrideObj.hasOwnProperty(key)) {
			if (result.hasOwnProperty(key)) {
				result[key]=MergeJSON(result[key], overrideObj[key]);
			} else {
				result[key]=overrideObj[key];
			}
        }
    }
    return result;
}

/*
var mergeJSON = function (target, add) {
    function isObject(obj) {
        if (typeof obj == "object") {
            for (var key in obj) {
                if (obj.hasOwnProperty(key)) {
                    return true; // search for first object prop
                }
            }
        }
        return false;
    }
    for (var key in add) {
        if (add.hasOwnProperty(key)) {
            if (target[key] && isObject(target[key]) && isObject(add[key])) {
                this.mergeJSON(target[key], add[key]);
            } else {
                target[key] = add[key];
            }
        }
    }
    return target;
};
*/


// Функция копирования JSON объекта
function CopyJSON( obj ) {
    return JSON.parse( JSON.stringify(obj) );
}

function CloneJSON(obj) {
    // basic type deep copy
    if (obj === null || obj === undefined || typeof obj !== 'object' || (obj instanceof Date))  {
        return obj
    }
    // array deep copy
    if (obj instanceof Array) {
        var cloneA = [];
        for (var i = 0; i < obj.length; ++i) {
            cloneA[i] = CloneJSON(obj[i]);
        }              
        return cloneA;
    }                  
    // object deep copy
    var cloneO = {};   
    for (var i in obj) {
        cloneO[i] = CloneJSON(obj[i]);
    }                  
    return cloneO;
}

function SchemaInherit( schema, childjson ) {
    var properties = MergeJSON( CopyJSON( schema.validator.$jsonSchema.properties ), CopyJSON( childjson ) );
    var fulljson = CopyJSON( schema );
    fulljson.validator.$jsonSchema.properties = properties;
/*    
    print("");
    print("");
    print("");
    printjson(fulljson);
    print("");
    print("");
    print("");
*/    
    return fulljson;
}

function CopySchemaType( schema_type, description ) {
    var result = CopyJSON(schema_type);
    result.description = description;
    return result;
}

// Транспонируем массив одинаковых элементов 
// в элемент массивов
function Transponire( items, defaultValue ){
    var newItems = {};
    var absentValue = (defaultValue ? defaultValue : 0);
    var count = 0;
    items.forEach(function(item){
        count++;
        for(var key in item) {
            if (!newItems[key]) {
                newItems[key] = [];
                // Выравниваем число элементов, если он до этого момента отсутствовал в структуре
                for(var j=0; j < (count-1); j++){
                    newItems[key].push(absentValue);
                }
            }
            newItems[key].push(item[key]);
        }
        // Выравниваем число элементов, если он отсутствовал в структуре
        for(var key in newItems) {
            if (newItems[key].length < count) {
                newItems[key].push(absentValue);
            }
        }
    });
    return newItems;
}

function RemoveFieldFromJSON(obj, fieldName) {
    if (typeof(obj) != "object") {
        return obj;
    }
    var result = {};
    for(var key in obj) {
        if((fieldName != key) && obj.hasOwnProperty(key)) {
            if ((typeof(obj[key]) == "object") && obj.propertyIsEnumerable(key)) {
                if (obj[key] && obj[key].length > 0) {
                    var arr = [];
                    obj[key].forEach( function(item) {
                        arr.push( RemoveFieldFromJSON( item, fieldName ) );
                    });
                    result[key] = arr;
                } else {
                    result[key] =  RemoveFieldFromJSON( obj[key], fieldName );
                }
            } else {
                result[key] = obj[key];
            }
        }
    }
    return result;
}

function IsEqual( a, b ) {
    if ((a == undefined) || (b == undefined))
        return false;
    if ((a == null) && (b == null))
        return true;
    if ((a == null) || (b == null))
        return false;
    var value1 = "";
    var value2 = "";
    if (typeof(a) == "string") {
        value1 = a;
    } else {
        value1 = JSON.stringify(a);
    }
    if (typeof(b) == "string") {
        value2 = b;
    } else {
        value2 = JSON.stringify(b);
    }
    return (value1.toUpperCase() === value2.toUpperCase());
}

function IsEqualTorrow( ta, tb ) {
    if ((ta == undefined) || (tb == undefined))
        return false;
    if ((ta == null) && (tb == null))
        return true;
    if ((ta == null) || (tb == null))
        return false;
	var a = ConvertToTorrowId(ta);
	var b = ConvertToTorrowId(tb);
    return ((a.s - b.s) == 0) && ((a.t - b.t) == 0) && a.o && b.o && (a.o.toString() == b.o.toString());
}

// Функция сравнения двух JSON элементов на индентичность
// compareNameOnly - игнорируются значения
// ordered - важен порядок полей
function ItemsEqual(item1, item2, compareNameOnly, ordered) {
	var countProperty1 = 0;
	var countProperty2 = 0;
    var keys1 = [];
	for(var key in item1) {
		if (item1.hasOwnProperty(key)) {
			countProperty1++;
            keys1.push( key );
		};
	};
	for(var key in item2) {
		if (item2.hasOwnProperty(key)) {
			countProperty2++;
			if(!item1.hasOwnProperty(key)) {
				return false;
			}
            if (ordered) {
                if (countProperty2 > keys1.length) {
                    return false;
                }
                if (!IsEqual(key, keys1[countProperty2-1])){
                    return false;
                }
            }
            if (compareNameOnly) {
                continue;
            }
            // Пробуем сравнить в лоб
            if (item1[key] != item2[key]) {
                if (typeof(item1[key]) != typeof(item2[key])) {
                    return false;
                }
                if (Array.isArray(item1[key]) ^ Array.isArray(item2[key])) {
                    // Если разные значения
                    return false;
                }
                if (typeof(item1[key]) == "object" && !Array.isArray(item1[key])) {
                    if (!ItemsEqual(item1[key], item2[key], compareNameOnly, ordered )){
                        return false;
                    }
                    continue;
                } 
                if (item1[key].toString() != item2[key].toString()) {
                    return false;
                }
            }
		}
	}
	return (countProperty1 == countProperty2);
};

function IsEqualObject( a, b ) {
    // Пока не сделано!!!!
    return false;
    
    if ((a == undefined) || (b == undefined))
        return false;
    if ((a == null) && (b == null))
        return true;
    if ((a == null) || (b == null))
        return false;
    if (typeof(a) == "object") {
        if (typeof(b) != "object") {
            return false;
        }
        for(var key in a) {
            if(a.hasOwnProperty(key)) {
                if (!b.hasOwnProperty(key)){
                    return false;
                }
                print( typeof(a[key]) );
                if ((typeof(a[key]) == "object")) {
                    if ((typeof(b[key]) != "object")) {
                        return false;
                    }
                    if (a.propertyIsEnumerable(key)) {
                        if (!b.propertyIsEnumerable(key)) {
                            return false;
                        }
                        print("2 " + key);
                        if (a[key] && a[key].length > 0) {
                            if ( !b[key] || a[key].length != b[key].length) {
                                return false;
                            }
                            var isEqual = true;
                            var i =0;
                            a[key].forEach( function(item) {
                                if (isEqual){
                                    if (!IsEqualTorrow( item, b[key][i] )) {
                                        isEqual = false;
                                    }
                                }
                                i++;
                            });
                            if (!isEqual) {
                                return false;
                            }
                        } else {
                            print("3 " + a[key]);
                            if (!IsEqualTorrow( a[key], b[key] )){
                                return false;
                            }
                        }
                    } else {
                        if (a[key].toString() != b[key].toString()) {
                            return false;
                        }
                    }
                } else {
                    print("!!! " + a[key]+ " " + b[key]);
                    if (!IsEqual(a[key], b[key])) {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }
        return true;
    } else if (typeof(a) == "string") {
        if (typeof(b) != "string") {
            return false;
        }
        return (a.toUpperCase() === b.toUpperCase());
    } else if ((typeof(a) == "int") || (typeof(a) == "long")) {
        if ((typeof(b) != "int") || (typeof(b) == "long")) {
            return false;
        }
        return (a === b);
    }
    return IsEqual(a[key], b[key]);
}


function sleep(milliseconds) {
  var start = new Date().getTime();
  for (var i = 0; i < 1e7; i++) {
    if ((new Date().getTime() - start) > milliseconds){
      break;
    }
  }
}


function StrLeftAlign( s, width ){
	if (typeof s == "undefined"){
		return " ".repeat(width);
	}
    return (s + " ".repeat(width)).slice(0,width);
};

function StrRightAlign( s, width ){
	if (typeof s == "undefined"){
		return " ".repeat(width);
	}
    return (" ".repeat(width) + s).slice(-width) ;
};

function StrRightAlign_Old( s, width ){
    return ((" ".repeat(width) + s).split(".")[0]).slice(-width);
};

function StrCenterAlign( s, width ){
	if (typeof s == "undefined"){
		return " ".repeat(width);
	}
    var n = " ".repeat(Math.ceil(width/2)) + s + " ".repeat(Math.ceil(width/2));
    return n.slice( Math.ceil((n.length - width)/2), Math.ceil((n.length - width)/2) + width);
};

function ArrayToRow( ar ) {
    if (typeof ar == "undefined")
        return "";
    var s = "";
    ar.forEach(function(item){
        s = s + item + " | ";
    });
    return s;
}

// Перевод числа (целого или дробного) в целое число и строку указанной длины
function NumberToIntString( num, len ) {
    return ((" ").repeat(len) + num.toString().split(".")[0]).slice(-len);
}

function NumberToDoubleString( num, len, dec ) {
    //
    var first = ((" ").repeat(len) + num.toString().split(".")[0] + ".").slice(dec-len);
    var second = ("0").repeat(dec);
    if (num.toString().split(".")[1]) {
        second = (num.toString().split(".")[1].substr(0,dec) + ("0").repeat(dec)).slice(0, dec);
    }
    return first + second;
//    return ((" ").repeat(len) + num.toString().split(".")[0] + "." + num.toString().split(".")[1].substr(0,dec)).slice(-len);
}


var TIMEZONE_OFFSET_MOSCOW = 3;
var DATETIME_FORMAT_FULL = NumberInt(1);
var DATETIME_FORMAT_NO_SECONDS = NumberInt(2);
var DATETIME_FORMAT_NO_MINUTES = NumberInt(3);
var DATETIME_FORMAT_NO_HOURS = NumberInt(4);

function DateTimeToString( d, offset, formatTime, formatDate ) {
    if (d === undefined || d == null) {
        return "null";
    }
    var myDate = new Date(d.getTime());
    if (offset) {
        myDate.setUTCHours(myDate.getUTCHours()+offset);
    }
	var timeValues = [];
	if (!IsEqual(formatTime, DATETIME_FORMAT_NO_HOURS)){
		timeValues.push(("00" + myDate.getUTCHours()).slice(-2));
		if (!IsEqual(formatTime, DATETIME_FORMAT_NO_MINUTES)){
			timeValues.push(("00" + myDate.getUTCMinutes()).slice(-2));
			if (!IsEqual(formatTime, DATETIME_FORMAT_NO_SECONDS)){
				timeValues.push(("00" + myDate.getUTCSeconds()).slice(-2));
			}
		}
	}
    return DateToString( d, offset, formatDate ) + (timeValues.length ? ' ' + timeValues.join(":") + " (UTC+"+(offset?offset:0)+")": "");
};

// t - millisec
// Offset - hour
function IntToTimeString( t ) {
    if (t === undefined || t == null) {
        return "null";
    }
    var myTime = t;
	var d = Math.floor( myTime/(24*60*60*1000) );
	var h = Math.floor( (myTime-24*d)/(60*60*1000) );
	var mi = Math.floor( (myTime-d*24-h*60)/(60*1000) );
	var sec = Math.floor( (myTime-d*24-h*60-mi*60)/1000 );
	var msec = Math.floor( myTime-d*24-h*60-mi*60-sec*60 );
    return (d ? d+" дн " : "") +
		[
            (h ? ("00" + h).slice(-2) + " час" : (d ? "00 час" : null)),
            (mi ? ("00" + mi).slice(-2) + " мин" : ( (d || h) ? "00 мин" : null)),
            (sec ? ("00" + sec).slice(-2) + " сек": ( (d || h || mi) ? "00 сек" : null)),
            (msec ? ("000" + msec).slice(-3) + " мсек" : "000 мсек")
        ].filter(i => i).join(" ");
};

var DATE_FORMAT_FULL = NumberInt(1);
var DATE_FORMAT_SHORT_YEAR = NumberInt(2);
var DATE_FORMAT_NO_YEAR = NumberInt(3);
var DATE_FORMAT_NO_MONTH = NumberInt(4);

function DateToString( d, offset, formatDate ) {
    if (d === undefined || d == null) {
        return "null";
    }
    var myDate = new Date(d.getTime());
    if (offset) {
        myDate.setUTCHours(myDate.getUTCHours()+offset);
    }
	var dateValues = [];
	dateValues.push(("00" + myDate.getUTCDate()).slice(-2));
	if (!IsEqual(formatDate, DATE_FORMAT_NO_MONTH)){
		dateValues.push(("00" + (myDate.getUTCMonth() + 1)).slice(-2));
		if (!IsEqual(formatDate, DATE_FORMAT_NO_YEAR)){
			if (!IsEqual(formatDate, DATE_FORMAT_SHORT_YEAR)){
				dateValues.push(myDate.getUTCFullYear().toString());
			} else {
				dateValues.push(myDate.getUTCFullYear().toString().slice(-2));
			}
		}
	}
    return dateValues.join('.');
};

var MINUTES_IN_HOUR = 60;
function TimezoneNameToMinutesOffset( timezoneName ){
	/* Список зон времени https://24timezones.com/time-zone/east
	db.tpUser.aggregate([
        {
            $group: {
                _id: "$tz",
                count: { $sum: 1 },
            }
		},
		{
			$sort: { 
				"_id": 1
			}
        }
	]);
	*/
	var TIMEZONE_OFFSET_MINUTES = [
		{ "tz" : "CEST", "offset" : 2*MINUTES_IN_HOUR },
		{ "tz" : "EEST", "offset" : -6*MINUTES_IN_HOUR },
		{ "tz" : "MESZ", "offset" : 2*MINUTES_IN_HOUR },
		{ "tz" : "Europe/Moscow", "offset" : 3*MINUTES_IN_HOUR },
		{ "tz" : "GMT+01:00", "offset" : 1*MINUTES_IN_HOUR },
		{ "tz" : "GMT+02:00", "offset" : 2*MINUTES_IN_HOUR },
		{ "tz" : "GMT+03:00", "offset" : 3*MINUTES_IN_HOUR },
		{ "tz" : "GMT+04:00", "offset" : 4*MINUTES_IN_HOUR },
		{ "tz" : "GMT+05:00", "offset" : 5*MINUTES_IN_HOUR },
		{ "tz" : "GMT+05:30", "offset" : 5.5*MINUTES_IN_HOUR},
		{ "tz" : "GMT+06:00", "offset" : 6*MINUTES_IN_HOUR },
		{ "tz" : "GMT+07:00", "offset" : 7*MINUTES_IN_HOUR },
		{ "tz" : "GMT+08:00", "offset" : 8*MINUTES_IN_HOUR },
		{ "tz" : "GMT+09:00", "offset" : 9*MINUTES_IN_HOUR },
		{ "tz" : "GMT+1", "offset" : 1*MINUTES_IN_HOUR },
		{ "tz" : "GMT+10", "offset" : 10*MINUTES_IN_HOUR },
		{ "tz" : "GMT+10:00", "offset" : 10*MINUTES_IN_HOUR },
		{ "tz" : "GMT+11", "offset" : 11*MINUTES_IN_HOUR },
		{ "tz" : "GMT+11:00", "offset" : 11*MINUTES_IN_HOUR },
		{ "tz" : "GMT+12", "offset" : 12*MINUTES_IN_HOUR },
		{ "tz" : "GMT+12:00", "offset" : 12*MINUTES_IN_HOUR },
		{ "tz" : "GMT+2", "offset" : 2*MINUTES_IN_HOUR },
		{ "tz" : "GMT+3", "offset" : 3*MINUTES_IN_HOUR },
		{ "tz" : "GMT+4", "offset" : 4*MINUTES_IN_HOUR },
		{ "tz" : "GMT+5", "offset" : 5*MINUTES_IN_HOUR },
		{ "tz" : "GMT+6", "offset" : 6*MINUTES_IN_HOUR },
		{ "tz" : "GMT+7", "offset" : 7*MINUTES_IN_HOUR },
		{ "tz" : "GMT+8", "offset" : 8*MINUTES_IN_HOUR },
		{ "tz" : "GMT+9", "offset" : 9*MINUTES_IN_HOUR },
		{ "tz" : "GMT-01:00", "offset" : -1*MINUTES_IN_HOUR },
		{ "tz" : "GMT-02:00", "offset" : -2*MINUTES_IN_HOUR },
		{ "tz" : "GMT-02:30", "offset" : -2.5*MINUTES_IN_HOUR },
		{ "tz" : "GMT-03:00", "offset" : -3*MINUTES_IN_HOUR },
		{ "tz" : "GMT-04:00", "offset" : -4*MINUTES_IN_HOUR },
		{ "tz" : "GMT-05:00", "offset" : -5*MINUTES_IN_HOUR },
		{ "tz" : "GMT-05:30", "offset" : -5.5*MINUTES_IN_HOUR},
		{ "tz" : "GMT-06:00", "offset" : -6*MINUTES_IN_HOUR },
		{ "tz" : "GMT-07:00", "offset" : -7*MINUTES_IN_HOUR },
		{ "tz" : "GMT-08:00", "offset" : -8*MINUTES_IN_HOUR },
		{ "tz" : "GMT-09:00", "offset" : -9*MINUTES_IN_HOUR },
		{ "tz" : "GMT-1", "offset" : -1*MINUTES_IN_HOUR },
		{ "tz" : "GMT-10", "offset" : -10*MINUTES_IN_HOUR },
		{ "tz" : "GMT-10:00", "offset" : -10*MINUTES_IN_HOUR },
		{ "tz" : "GMT-11", "offset" : -11*MINUTES_IN_HOUR },
		{ "tz" : "GMT-11:00", "offset" : -11*MINUTES_IN_HOUR },
		{ "tz" : "GMT-12", "offset" : -12*MINUTES_IN_HOUR },
		{ "tz" : "GMT-12:00", "offset" : -12*MINUTES_IN_HOUR },
		{ "tz" : "GMT-2", "offset" : -2*MINUTES_IN_HOUR },
		{ "tz" : "GMT-3", "offset" : -3*MINUTES_IN_HOUR },
		{ "tz" : "GMT-4", "offset" : -4*MINUTES_IN_HOUR },
		{ "tz" : "GMT-5", "offset" : -5*MINUTES_IN_HOUR },
		{ "tz" : "GMT-6", "offset" : -6*MINUTES_IN_HOUR },
		{ "tz" : "GMT-7", "offset" : -7*MINUTES_IN_HOUR },
		{ "tz" : "GMT-8", "offset" : -8*MINUTES_IN_HOUR },
		{ "tz" : "GMT-9", "offset" : -9*MINUTES_IN_HOUR }
	];
	if (!timezoneName) {
		return null;
	}
	var compare = timezoneName.toUpperCase();
	var timezone = TIMEZONE_OFFSET_MINUTES.filter(i => IsEqual(i.tz, compare))[0];
	if (!timezone) {
		print("WARNING TimezoneNameToOffset: Timezone name <"+timezoneName+"> has unknown offset.");
		return null;
	}
	return timezone.offset;
};

function GetNearestTimeInterval( intervals, minutesOffset, testUTCNowTime){
	var minTime = null;
	intervals.forEach(function(interval){
		var result = GetNearestTime( interval.fromHours, interval.toHours, minutesOffset, testUTCNowTime);
		if (!result){
			return;
		}
		if (!minTime) {
			minTime = result;
		}
		if (minTime > result){
			minTime = result;
		}
	});
	return minTime;
}

function GetNearestTime( sendIntervalHourFrom, sendIntervalHourTo, minutesOffset, testUTCNowTime){
	if (sendIntervalHourFrom > sendIntervalHourTo) {
		print("ERROR GetNearestTime: sendIntervalHourFrom должен быть меньше sendIntervalHourTo");
		return null;
	}
	if (sendIntervalHourFrom < 0) {
		print("ERROR GetNearestTime: sendIntervalHourFrom должен быть больше 0");
		return null;
	}
	if (sendIntervalHourTo > 24) {
		print("ERROR GetNearestTime: sendIntervalHourTo должен быть меньше или равен 24");
		return null;
	}
	var nowTime = (testUTCNowTime ? new Date(testUTCNowTime.getTime()) : new Date());
	var userTime = nowTime.getUTCHours()*MINUTES_IN_HOUR + nowTime.getUTCMinutes() + minutesOffset;
	var addOffset = 0;
	if (userTime < 0){
		userTime = userTime + 24*MINUTES_IN_HOUR;
	}
	if (userTime > 24*MINUTES_IN_HOUR){
		userTime = userTime - 24*MINUTES_IN_HOUR;
	}
	if (userTime < sendIntervalHourFrom*MINUTES_IN_HOUR) {
		addOffset += sendIntervalHourFrom*MINUTES_IN_HOUR - userTime;
	}
	if (userTime > sendIntervalHourTo*MINUTES_IN_HOUR) {
		addOffset += (24*MINUTES_IN_HOUR - userTime) + sendIntervalHourFrom*MINUTES_IN_HOUR;
	}
	return nowTime.addMinutes(addOffset);
}

function TestGetNearestTime(){
	var count = 0;
	var errorCount = 0;
	[
		{	// 1
			sendIntervalHourFrom: 10,
			sendIntervalHourTo: 12,
			offsetMin: 3*MINUTES_IN_HOUR,
			testUTCNowTime: new Date(Date.UTC(2022, 0, 1, 8, 30, 0)),
			resultUTCTime: new Date(Date.UTC(2022, 0, 1, 8, 30, 0))
		},
		{	// 2
			sendIntervalHourFrom: 10,
			sendIntervalHourTo: 12,
			offsetMin: 3*MINUTES_IN_HOUR,
			testUTCNowTime: new Date(Date.UTC(2022, 0, 1, 6, 30, 0)),
			resultUTCTime: new Date(Date.UTC(2022, 0, 1, 7, 00, 0))
		},
		{	// 3
			sendIntervalHourFrom: 10,
			sendIntervalHourTo: 12,
			offsetMin: 3*MINUTES_IN_HOUR,
			testUTCNowTime: new Date(Date.UTC(2022, 0, 1, 12, 30, 0)),
			resultUTCTime: new Date(Date.UTC(2022, 0, 2, 7, 0, 0))
		},
		{	// 4
			sendIntervalHourFrom: 10,
			sendIntervalHourTo: 12,
			offsetMin: 3*MINUTES_IN_HOUR,
			testUTCNowTime: new Date(Date.UTC(2022, 0, 1, 23, 30, 0)),
			resultUTCTime: new Date(Date.UTC(2022, 0, 2, 7, 0, 0))
		},
		{	// 5
			sendIntervalHourFrom: 10,
			sendIntervalHourTo: 12,
			offsetMin: -10*MINUTES_IN_HOUR,
			testUTCNowTime: new Date(Date.UTC(2022, 0, 1, 23, 30, 0)),
			resultUTCTime: new Date(Date.UTC(2022, 0, 2, 20, 0, 0))
		},
		{	// 6
			sendIntervalHourFrom: 10,
			sendIntervalHourTo: 12,
			offsetMin: -10*MINUTES_IN_HOUR,
			testUTCNowTime: new Date(Date.UTC(2022, 0, 2, 03, 30, 0)),
			resultUTCTime: new Date(Date.UTC(2022, 0, 2, 20, 0, 0))
		},
		{	// 7
			sendIntervalHourFrom: 10,
			sendIntervalHourTo: 22,
			offsetMin: -10*MINUTES_IN_HOUR,
			testUTCNowTime: new Date(Date.UTC(2022, 0, 2, 03, 30, 0)),
			resultUTCTime: new Date(Date.UTC(2022, 0, 2, 03, 30, 0))
		},
	].forEach(function(test){
		count++;
		print("First tests "+count);
		var resultUTCTime = GetNearestTime( test.sendIntervalHourFrom, test.sendIntervalHourTo, test.offsetMin, test.testUTCNowTime);
		if (!TestHelper_Assert(resultUTCTime.getTime() == test.resultUTCTime.getTime(), "ERROR First Test "+count+". Получено время "+DateTimeToString(resultUTCTime)+", а ожидается время " + DateTimeToString(test.resultUTCTime))){
			errorCount++;
		}
	});
	print("");
	print("В "+count+" GetNearestTime тестах "+errorCount+" ошибок.");

	print("");
	count = 0;
	errorCount = 0;	
	[
		{	// 1
			intervals: [
				{
					fromHours: 10,
					toHours: 12
				},
				{
					fromHours: 15,
					toHours: 18
				}
			],
			offsetMin: 3*MINUTES_IN_HOUR,
			testUTCNowTime: new Date(Date.UTC(2022, 0, 1, 06, 30, 0)),
			resultUTCTime: new Date(Date.UTC(2022, 0, 1, 07, 00, 0))
		},
		{	// 2
			intervals: [
				{
					fromHours: 10,
					toHours: 12
				},
				{
					fromHours: 15,
					toHours: 18
				}
			],
			offsetMin: 3*MINUTES_IN_HOUR,
			testUTCNowTime: new Date(Date.UTC(2022, 0, 1, 08, 30, 0)),
			resultUTCTime: new Date(Date.UTC(2022, 0, 1, 08, 30, 0))
		},
		{	// 3
			intervals: [
				{
					fromHours: 10,
					toHours: 12
				},
				{
					fromHours: 15,
					toHours: 18
				}
			],
			offsetMin: 3*MINUTES_IN_HOUR,
			testUTCNowTime: new Date(Date.UTC(2022, 0, 1, 10, 30, 0)),
			resultUTCTime: new Date(Date.UTC(2022, 0, 1, 12, 00, 0))
		},
	].forEach(function(test){
		count++;
		print("Second tests "+count);
		var resultUTCTime = GetNearestTimeInterval( test.intervals, test.offsetMin, test.testUTCNowTime);
		if (!TestHelper_Assert(resultUTCTime.getTime() == test.resultUTCTime.getTime(), "ERROR Second Test "+count+". Получено время "+DateTimeToString(resultUTCTime)+", а ожидается время " + DateTimeToString(test.resultUTCTime))){
			errorCount++;
		}
	});
	print("");
	print("В "+count+" GetNearestTimeInterval тестах "+errorCount+" ошибок.");
	print("");
}

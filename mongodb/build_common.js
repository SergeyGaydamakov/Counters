// Функция для получения конфигурации кластера из файла
const CONFIG_FILE_NAME = "./build_config.json";
function getConfigJSON( filename = CONFIG_FILE_NAME ){
	const fs = require('fs');
    let config = null;
    try {
        config = JSON.parse(fs.readFileSync(CONFIG_FILE_NAME));
    } catch (error) {
        console.log("ERROR reading of config <"+CONFIG_FILE_NAME+">: "+ error.message);
        return null;
    }
    if (!config) {
        console.log("ERROR: Config <"+CONFIG_FILE_NAME+"> is null");
        return null;
    }
    return config;
}

// Функция для создания администратора набора реплик
function createAdminUserOfCluster( user_name, user_password, hostname ) {
    if (!user_name) {
        console.log("WARNING: Admin user is empty for host. Security is not configured.");
        return true;
    }

    // Сначала пробуем аутентифицироваться, так как могли запустить скрипт повторно
    // Переключаемся на базу данных admin;
    db = db.getSiblingDB("admin");
    console.log("Trying authentificate as user <"+user_name+"> on <"+hostname+">.");
	try {
		if (db.auth( user_name, user_password)) {
			console.log("Success authentificate as user <"+user_name+"> on <"+hostname+">.");
		} else {
			// console.log("This error is Ok! We try to authentificate as user <"+user_name+"> on <"+hostname+"> for deciding to create this user.");
		}
    } catch (e) {
		// console.log(e.message);
		// console.log("This error is Ok! We try to authentificate as user <"+user_name+"> on <"+hostname+"> for deciding to create this user.");
    }
    try {
        if (db.getSiblingDB("admin").getUser( user_name )) {
            console.log("User <"+user_name+"> on <"+hostname+"> has been already created.");
            return true;
        }
    } catch (e) {
        // console.log("It is Ok! We want, but can not get user info <"+user_name+">: "+ e.message);
    }
	try {
		db.getSiblingDB("admin").createUser( { "user": user_name, "pwd": user_password, roles:[{"role":"userAdminAnyDatabase","db":"admin"}, {"role":"clusterAdmin","db":"admin"}, {"role":"root","db":"admin"}] } );
    } catch (e) {
        console.log("***ERROR*** create admin user <"+user_name+"> on <"+hostname+">   : " + e.message);
        console.log("Connect to server and solve problems.");
        return false;
    }
	try {
		if (db.auth( user_name, user_password)) {
			console.log("Success checking authentificate as user <"+user_name+"> on <"+hostname+">.");
		} else {
			console.log("***ERROR*** Can not authentificate as user <"+user_name+"> on <"+hostname+"> after create admin user.");
			return false;
		}
    } catch (e) {
		console.log(e.message);
		console.log("***ERROR*** Can not authentificate as user <"+user_name+"> on <"+hostname+"> after create admin user.");
		return false;
    }
    console.log("***SUCCESS*** Admin user <"+user_name+"> on <"+hostname+"> has created successfully.");
    return true;
}

// Функция для инициализации набора реплик
function build(replicaName){
	console.log("Start initiating replica set <"+replicaName+">");
    const config = getConfigJSON();
    if (!config) {
        console.log("ERROR: Config is null");
        return false;
    }
    const cfg = config.replicaSets.find(cfg => cfg._id === replicaName);
    if (!cfg) {
        console.log("ERROR: Replica set <"+replicaName+"> not found in config");
        return false;
    }
    const admin_name = config.admin_name;
    const admin_password = config.admin_password;
	// console.log( JSON.stringify(cfg, null, 2) );

	try {
        // Проверка, является ли сервер репликой
        const info = db.isMaster();
        const isReplicaset = info.ismaster || info.secondary;
        if (!isReplicaset ) {
		    rs.initiate( cfg );
        }
        // Ожидание инициализации набора реплик
        const timeoutTime = new Date(Date.now() + 20000);
        let ismaster = false;
        while (!ismaster && (timeoutTime.getTime() > new Date().getTime())) {
            // Ожидаем 15 секунд
            sleep(1000);
            ismaster = db.isMaster().ismaster;
        }
        if (!ismaster){
            console.log(`ERROR: Ошибка инициализации набора реплик ${cfg._id}`);
            return false;
        }

        // Создание пользователя
        if (!createAdminUserOfCluster( admin_name, admin_password, cfg._id )) {
            console.log("");
            return false;
        }
    } catch (e) {
        console.log("***ERROR*** in rs.initiate: " + e.message);
        return false;
    };
    console.log("***SUCCESS*** Replica set <"+cfg._id+"> was initiated successfully.");
    console.log("");
    return true;
};

// Создание кластера из готовых Replica Set
function createCluster(){
    console.log("Creating shard cluster:");
    const config = getConfigJSON();
    if (!config) {
        console.log("ERROR: Config is null");
        return false;
    }
    const admin_name = config.admin_name;
    const admin_password = config.admin_password;
    const shards = config.replicaSets.filter(shard => shard._id !== config.configReplicaSet).map(shard => {
        return {
            _id: shard._id,
            hosts: shard.members.map(member => member.host).join(",")
        }
    });

    if (!createAdminUserOfCluster( admin_name, admin_password, "mongos")) {
        console.log("***ERROR*** Step of creating cluster has errors");
        return false;
    }

	try {
		var res = db.adminCommand({
			setDefaultRWConcern : 1,
			defaultWriteConcern: { w: "majority" },
			comment: "Value set at creating cluster"
		});
		if (!res.ok) {
			console.log("ERROR: defaultWriteConcern was not setted to "+"majority");
			console.log(res);
			return false;
		}
	} catch (e) {
		console.log(e.message);
		console.log("ERROR: defaultWriteConcern was not setted to "+"majority");
		return false;
	}
	console.log("defaultWriteConcern was setted to "+"majority");
	//
    var listShards = db.adminCommand( { "listShards": 1 } );
    if (!listShards.ok) {
        console.log("ERROR: Can not get listShard: " + listShards.errmsg);
        return false;
    }
    if (listShards.shards.length) {
        console.log("Cluster has already shards: ", listShards.shards.map(shard => shard._id+"("+shard.host+ " state "+shard.state+")").join(", "));
        return success;
    } else {
        console.log("Cluster has no shards");
    }

    console.log("Add shards to cluster:");
    var success = true;
    shards.forEach( function( shard ){
        var res = sh.addShard(shard._id + "/" +shard.hosts);
        if (!res.ok){
            console.log("ERROR in sh.addShard: " + res.errmsg + " (code = " + res.code + ", code name = "+res.codeName+")");
            success = false;
        } else {
            console.log("Shard " + shard._id + " added success.");
        }
    });
    if (success) {
        console.log("***SUCCESS*** Shard cluster has been created success.");
    } else {
        console.log("***ERROR*** Shard cluster has been created with errors. Connect to cluster and solve problems.");
    }
    return success;
}

module.exports = {
    build,
    createCluster
};
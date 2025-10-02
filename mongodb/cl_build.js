load("common.js");

const user_name = "admin";
const user_password = "admin";

const shards = [
    {
        "_id": "rs01",
        "hosts": "localhost:29101"
    },
    {
        "_id": "rs02",
        "hosts": "localhost:29201"
    },
    {
        "_id": "rs03",
        "hosts": "localhost:29301"
    }
];

(function(){
    if (!createAdminUserOfCluster( user_name, user_password, "mongos")) {
        console.log("***ERROR*** Step of creating cluster has errors");
        return false;
    }
    CreateCluster(shards);
    console.log("");
})();

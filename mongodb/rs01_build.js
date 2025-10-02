load("common.js");

const user_name = "admin";
const user_password = "admin";

const cfg = {
    "_id": "rs01",
    "writeConcernMajorityJournalDefault" : true,
    "members" : [
        {
                "_id" : 0,
                "host" : "localhost:29101",
                "arbiterOnly" : false,
                "buildIndexes" : true,
                "hidden" : false,
                "priority" : 10,
                "tags" : {

                },
                "secondaryDelaySecs" : 0,
                "votes" : 1
        },
    ],
    "settings" : {
        "chainingAllowed" : true,
        "getLastErrorModes" : {

        },
        "getLastErrorDefaults" : {
                "w" : 1,
                "wtimeout" : 0
        }
    }
};

(function(){
    build(cfg, user_name, user_password);
})();

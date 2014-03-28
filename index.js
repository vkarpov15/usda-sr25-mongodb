var async = require('async'),
    fs = require('fs'),
    mongodb = require('mongodb'),
    rowMappings = require('./row_mappings.js'),
    _ = require('underscore');

var COLUMN_DELIMITER = '^';
var STRING_DELIMITER = '~';

mongodb.connect("mongodb://localhost:27017/usda", function(error, db) {
  conn = db;
  var collection = db.collection("nutrition");

  run(collection);
});

var parseLine = function(line, mappings) {
  var cols = line.split(COLUMN_DELIMITER);

  var ret = {};
  for (key in mappings) {
    var v = cols[mappings[key]];
    if (v.charAt(0) == STRING_DELIMITER && v.charAt(v.length - 1) == STRING_DELIMITER) {
      v = v.substring(1, v.length - 1);
    } else {
      v = parseFloat(v);
    }

    ret[key] = v;
  }

  return ret;
};

var foods = {};
var nutrients = {};

var run = function(collection) {
  var foodDesContent = fs.readFileSync("./data/FOOD_DES.txt");
  var lines = foodDesContent.toString().trim().split("\n");
  _.each(lines, function(line) {
    var doc = parseLine(line, rowMappings.FOOD_DES_MAPPING);
    doc.nutrients = [];
    doc.weights = [];
    foods[doc._id] = doc;
  });

  var weightContent = fs.readFileSync("./data/WEIGHT.txt");
  var lines = weightContent.toString().trim().split("\n");
  _.each(lines, function(line) {
    var doc = parseLine(line, rowMappings.WEIGHT_MAPPING);
    var newDoc = _.clone(doc);
    delete newDoc._id;
    foods[doc._id].weights.push(newDoc);
  });

  var nutrDefContent = fs.readFileSync("./data/NUTR_DEF.txt");
  lines = nutrDefContent.toString().trim().split("\n");
  _.each(lines, function(line) {
    var doc = parseLine(line, rowMappings.NUTR_DEF_MAPPING);
    nutrients[doc._id] = doc;
  });

  var foodNutrientMappings = fs.readFileSync('./data/NUT_DATA.txt');
  lines = foodNutrientMappings.toString().trim().split("\n");
  _.each(lines, function(line) {
    var doc = parseLine(line, rowMappings.NUT_DATA_MAPPING);
    var subDoc = _.clone(nutrients[doc.nutrient]);
    subDoc.amountPer100G = doc.amountPer100G;

    foods[doc.food].nutrients.push(subDoc);
  });

  var saveFns = [];
  _.each(foods, function(food) {
    saveFns.push(function(callback) {
      collection.insert(food, function(error, result) {
        if (error) {
          console.log("Error saving " + food._id);
        } else {
          console.log("Inserted " + food._id);
        }
        callback(error, result);
      });
    });
  });

  async.series(saveFns, function(error, callback) {
    console.log("done");
  });
};
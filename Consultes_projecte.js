
//1. Les 5 publicacions amb major preu. Mostrar només el títol i preu.
db.publicacions.find({}, {_id:0, titol:1, preu:1}).sort({preu: -1}).limit(5)

//2. Valor màxim, mínim i mitjà del preus de les publicacions de l’editorial Juniper Books.
db.publicacions.aggregate([
    {
    $match:{editorial: "Juniper Books"}
    },
    {
    $group: {_id: "$editorial",preu_max: { $max: "$preu" },preu_min: { $min: "$preu" },preu_mig: { $avg: "$preu" }}
    }
])

//3. Artistes (nom artístic) que participen en més de 5 publicacions com a dibuixant.
db.publicacions.aggregate([
  {$unwind: "$dibuixants"},
  {$group: {_id: "$dibuixants", count: {$sum: 1}}},
  {$match: {count: {$gt: 5}}},
  {$project: {_id: 1}}
])

//4. Número de col·leccions per gènere. Mostra gènere i número total.
db.coleccions.aggregate([{$unwind: "$genere"},
  { $group: { _id: "$genere", total: { $sum: 1 } } },
  { $project: { total: 1 } }
])

//5. Per cada editorial, mostrar el recompte de col·leccions finalitzades i no finalitzades.
db.coleccions.aggregate([
    {
    $unwind: "$editorials"
    }, 
    {
    $group:{
        _id: "$editorials",
        finalitzades : {$sum: { $cond:{ if :{ $eq: ["$tancada", true] }, then: 1, else: 0}}},
        no_finalitzades : {$sum: { $cond:{ if :{ $eq: ["$tancada", true] }, then: 0,else: 1}}}
        }
    }
])

//6. Mostrar les 2 col·leccions ja finalitzades amb més publicacions. Mostrar editorial i nom col·lecció.
db.coleccions.aggregate([
    {$match:{tancada:true}},
    {$sort:{total_exemplars:-1}},
    {
    $unwind:"$editorials"
    },
    {$limit:2},
    {$project:{_id:1,editorials:1}}
])

//7. Mostrar el país d’origen de l’artista o artistes que han fet més guions.
db.artistes.aggregate([
    {
    $lookup:{
        from:"publicacions",
        localField:"_id",
        foreignField:"guionistes",
        as:"guions"
    }
    },
    {$addFields:{num_guions:{$size:"$guions"}}},
    {$sort:{num_guions:-1}},
    {$limit:1},
    {$project:{_id:0,pais:1}}
])

//8. Mostrar les publicacions amb tots els personatges de tipus “heroe”.
db.publicacions.aggregate([
    {
    $lookup:{
        from: "personatges",
        localField: "_id",
        foreignField: "isbn",
        as: "personatges"
        }
    },
    {
    $match:{$and:[{"personatges.0":{$exists:true}},{personatges:{$not:{$elemMatch:{tipus:{$ne:"heroe"}}}}}]}
    },
    {$project:{_id:1}}
])

//9. Modificar el preu de les publicacions amb stock superior a 20 exemplars i incrementar-lo un 25%.
db.publicacions.updateMany({stock: {$gt: 20}},{$mul: {preu: 1.25}})
db.publicacions.find()

//10. Mostrar ISBN i títol de les publicacions conjuntament amb tota la seva informació dels personatges.
db.publicacions.aggregate([
  {
    $lookup: {
      from: "personatges",
      localField: "_id",
      foreignField: "isbn",
      as: "personatges"
    }
  },
  {$project: {_id: 1,titol: 1,personatges: 1}}
])









package cognite.spark.v1

object RawTableRelationSupportingTestData {

  val starships = Seq(
    ("key01", "Millennium Falcon", "YT-1300 light freighter", "Light freighter"),
    ("key02", "X-wing", "T-65 X-wing starfighter", "Starfighter"),
    ("key03", "TIE Fighter", "Twin Ion Engine/Ln Starfighter", "Starfighter"),
    ("key04", "Star Destroyer", "Imperial I-class Star Destroyer", "Capital ship"),
    ("key05", "Slave 1", "Firespray-31-class patrol and attack craft", "Patrol craft"),
    ("key06", "A-wing", "RZ-1 A-wing interceptor", "Interceptor"),
    ("key07", "B-wing", "A/SF-01 B-wing starfighter", "Assault starfighter"),
    ("key08", "Y-wing", "BTL Y-wing starfighter", "Assault starfighter"),
    ("key09", "Executor", "Executor-class Star Dreadnought", "Star Dreadnought"),
    ("key10", "Rebel transport", "GR-75 medium transport", "Medium transport"),
    ("key11", "Naboo Royal Starship", "J-type 327 Nubian", "Yacht"),
    ("key12", "ARC-170", "Aggressive ReConnaissance-170 starfighter", "Starfighter"),
    ("key13", "Eta-2 Actis", "Eta-2 Actis-class light interceptor", "Interceptor"),
    ("key14", "Venator-class Star Destroyer", "Venator-class", "Capital ship"),
    ("key15", "Naboo N-1 Starfighter", "N-1", "Starfighter"),
    ("key16", "Jedi Interceptor", "Eta-2 Actis-class interceptor", "Interceptor"),
    ("key17", "Sith Infiltrator", "Scimitar", "Starfighter"),
    ("key18", "V-wing", "Alpha-3 Nimbus-class V-wing starfighter", "Starfighter"),
    ("key19", "Delta-7 Aethersprite", "Delta-7 Aethersprite-class light interceptor", "Interceptor"),
    ("key20", "Imperial Shuttle", "Lambda-class T-4a shuttle", "Shuttle"),
    ("key21", "Tantive IV", "CR90 corvette", "Corvette"),
    ("key22", "Slave II", "Firespray-31-class patrol and attack craft", "Patrol craft"),
    ("key23", "TIE Bomber", "TIE/sa bomber", "Bomber"),
    ("key24", "Imperial Star Destroyer", "Imperial I-class Star Destroyer", "Capital ship"),
    ("key25", "Sith Speeder", "FC-20 speeder bike", "Speeder"),
    ("key26", "Speeder Bike", "74-Z speeder bike", "Speeder"),
    ("key27", "Solar Sailer", "Punworcca 116-class interstellar sloop", "Sloop"),
    ("key28", "Geonosian Starfighter", "Nantex-class territorial defense starfighter", "Starfighter"),
    ("key29", "Hound's Tooth", "YT-2000 light freighter", "Light freighter"),
    ("key30", "Scimitar", "Sith Infiltrator", "Starfighter"),
    ("key31", "Tie Interceptor", "TIE/in interceptor", "Starfighter"),
    ("key32", "Naboo Royal Cruiser", "J-type diplomatic barge", "Yacht"),
    ("key33", "X-34 Landspeeder", "X-34", "Landspeeder"),
    ("key34", "Snowspeeder", "T-47 airspeeder", "Airspeeder"),
    ("key35", "The Ghost", "VCX-100 light freighter", "Light freighter"),
    ("key36", "Phantom", "VCX-series auxiliary starfighter", "Auxiliary starfighter"),
    ("key37", "Outrider", "YT-2400 light freighter", "Light freighter"),
    ("key38", "Razor Crest", "ST-70 Assault Ship", "Assault ship"),
    ("key39", "Naboo Yacht", "J-type star skiff", "Yacht"),
    ("key40", "U-wing", "UT-60D", "Transport"),
    ("key41", "TIE Advanced x1", "TIE Advanced x1", "Starfighter"),
    ("key42", "J-type 327 Nubian", "J-type 327", "Yacht"),
    ("key43", "Naboo Royal Starship", "J-type 327 Nubian", "Yacht"),
    ("key44", "Naboo N-1 Starfighter", "N-1", "Starfighter"),
    ("key45", "Sith Infiltrator", "Scimitar", "Starfighter"),
    ("key46", "Havoc Marauder", "Omicron-class attack shuttle", "Attack shuttle"),
    ("key47", "Luthen's Ship", "Fondor Haulcraft", "Haulcraft"),
    ("key48", "Tantive IV", "CR90 corvette", "Corvette"),
    ("key49", "Millennium Falcon", "YT-1300 light freighter", "Light freighter"),
    ("key50", "Jedi Starfighter", "Delta-7 Aethersprite-class light interceptor", "Interceptor"),
    ("key51", "Vulture Droid", "Variable Geometry Self-Propelled Battle Droid", "Starfighter"),
    ("key52", "Tri-Fighter", "Droid Tri-Fighter", "Starfighter"),
    ("key53", "Hyena Bomber", "Baktoid Armor Workshop", "Starfighter"),
    ("key54", "Droid Gunship", "Heavy Missile Platform", "Gunship"),
    ("key55", "Malevolence", "Subjugator-class heavy cruiser", "Heavy cruiser"),
    ("key56", "Invisible Hand", "Providence-class Dreadnought", "Dreadnought"),
    ("key57", "Malevolence", "Subjugator-class heavy cruiser", "Heavy cruiser"),
    ("key58", "Invisible Hand", "Providence-class Dreadnought", "Dreadnought"),
    ("key59", "Droid Control Ship", "Lucrehulk-class battleship", "Battleship"),
    ("key60", "Venator-class Star Destroyer", "Venator-class", "Capital ship"),
  )

  val starshipsMap: Map[String, Map[String, String]] =
    starships.map { s => (s._1 -> Map("name" -> s._2, "model" -> s._3, "class" -> s._4)) }.toMap
}

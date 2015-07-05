package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RandomTextWriter extends Configured implements Tool {
	private static String[] words = { "diurnalness", "Homoiousian",
			"spiranthic", "tetragynian", "silverhead", "ungreat", "lithograph",
			"exploiter", "physiologian", "by", "hellbender", "Filipendula",
			"undeterring", "antiscolic", "pentagamist", "hypoid", "cacuminal",
			"sertularian", "schoolmasterism", "nonuple", "gallybeggar",
			"phytonic", "swearingly", "nebular", "Confervales",
			"thermochemically", "characinoid", "cocksuredom", "fallacious",
			"feasibleness", "debromination", "playfellowship", "tramplike",
			"testa", "participatingly", "unaccessible", "bromate",
			"experientialist", "roughcast", "docimastical", "choralcelo",
			"blightbird", "peptonate", "sombreroed", "unschematized",
			"antiabolitionist", "besagne", "mastication", "bromic",
			"sviatonosite", "cattimandoo", "metaphrastical", "endotheliomyoma",
			"hysterolysis", "unfulminated", "Hester", "oblongly",
			"blurredness", "authorling", "chasmy", "Scorpaenidae",
			"toxihaemia", "Dictograph", "Quakerishly", "deaf", "timbermonger",
			"strammel", "Thraupidae", "seditious", "plerome", "Arneb",
			"eristically", "serpentinic", "glaumrie", "socioromantic",
			"apocalypst", "tartrous", "Bassaris", "angiolymphoma", "horsefly",
			"kenno", "astronomize", "euphemious", "arsenide", "untongued",
			"parabolicness", "uvanite", "helpless", "gemmeous", "stormy",
			"templar", "erythrodextrin", "comism", "interfraternal",
			"preparative", "parastas", "frontoorbital", "Ophiosaurus",
			"diopside", "serosanguineous", "ununiformly", "karyological",
			"collegian", "allotropic", "depravity", "amylogenesis",
			"reformatory", "epidymides", "pleurotropous", "trillium",
			"dastardliness", "coadvice", "embryotic", "benthonic",
			"pomiferous", "figureheadship", "Megaluridae", "Harpa", "frenal",
			"commotion", "abthainry", "cobeliever", "manilla", "spiciferous",
			"nativeness", "obispo", "monilioid", "biopsic", "valvula",
			"enterostomy", "planosubulate", "pterostigma", "lifter",
			"triradiated", "venialness", "tum", "archistome", "tautness",
			"unswanlike", "antivenin", "Lentibulariaceae", "Triphora",
			"angiopathy", "anta", "Dawsonia", "becomma", "Yannigan",
			"winterproof", "antalgol", "harr", "underogating", "ineunt",
			"cornberry", "flippantness", "scyphostoma", "approbation", "Ghent",
			"Macraucheniidae", "scabbiness", "unanatomized", "photoelasticity",
			"eurythermal", "enation", "prepavement", "flushgate",
			"subsequentially", "Edo", "antihero", "Isokontae", "unforkedness",
			"porriginous", "daytime", "nonexecutive", "trisilicic",
			"morphiomania", "paranephros", "botchedly", "impugnation",
			"Dodecatheon", "obolus", "unburnt", "provedore", "Aktistetae",
			"superindifference", "Alethea", "Joachimite", "cyanophilous",
			"chorograph", "brooky", "figured", "periclitation", "quintette",
			"hondo", "ornithodelphous", "unefficient", "pondside", "bogydom",
			"laurinoxylon", "Shiah", "unharmed", "cartful", "noncrystallized",
			"abusiveness", "cromlech", "japanned", "rizzomed", "underskin",
			"adscendent", "allectory", "gelatinousness", "volcano",
			"uncompromisingly", "cubit", "idiotize", "unfurbelowed",
			"undinted", "magnetooptics", "Savitar", "diwata", "ramosopalmate",
			"Pishquow", "tomorn", "apopenptic", "Haversian", "Hysterocarpus",
			"ten", "outhue", "Bertat", "mechanist", "asparaginic", "velaric",
			"tonsure", "bubble", "Pyrales", "regardful", "glyphography",
			"calabazilla", "shellworker", "stradametrical", "havoc",
			"theologicopolitical", "sawdust", "diatomaceous", "jajman",
			"temporomastoid", "Serrifera", "Ochnaceae", "aspersor",
			"trailmaking", "Bishareen", "digitule", "octogynous",
			"epididymitis", "smokefarthings", "bacillite", "overcrown",
			"mangonism", "sirrah", "undecorated", "psychofugal",
			"bismuthiferous", "rechar", "Lemuridae", "frameable",
			"thiodiazole", "Scanic", "sportswomanship", "interruptedness",
			"admissory", "osteopaedion", "tingly", "tomorrowness",
			"ethnocracy", "trabecular", "vitally", "fossilism", "adz",
			"metopon", "prefatorial", "expiscate", "diathermacy", "chronist",
			"nigh", "generalizable", "hysterogen", "aurothiosulphuric",
			"whitlowwort", "downthrust", "Protestantize", "monander", "Itea",
			"chronographic", "silicize", "Dunlop", "eer", "componental",
			"spot", "pamphlet", "antineuritic", "paradisean", "interruptor",
			"debellator", "overcultured", "Florissant", "hyocholic",
			"pneumatotherapy", "tailoress", "rave", "unpeople", "Sebastian",
			"thermanesthesia", "Coniferae", "swacking", "posterishness",
			"ethmopalatal", "whittle", "analgize", "scabbardless", "naught",
			"symbiogenetically", "trip", "parodist", "columniform", "trunnel",
			"yawler", "goodwill", "pseudohalogen", "swangy", "cervisial",
			"mediateness", "genii", "imprescribable", "pony", "consumptional",
			"carposporangial", "poleax", "bestill", "subfebrile", "sapphiric",
			"arrowworm", "qualminess", "ultraobscure", "thorite", "Fouquieria",
			"Bermudian", "prescriber", "elemicin", "warlike", "semiangle",
			"rotular", "misthread", "returnability", "seraphism", "precostal",
			"quarried", "Babylonism", "sangaree", "seelful", "placatory",
			"pachydermous", "bozal", "galbulus", "spermaphyte", "cumbrousness",
			"pope", "signifier", "Endomycetaceae", "shallowish", "sequacity",
			"periarthritis", "bathysphere", "pentosuria", "Dadaism",
			"spookdom", "Consolamentum", "afterpressure", "mutter", "louse",
			"ovoviviparous", "corbel", "metastoma", "biventer", "Hydrangea",
			"hogmace", "seizing", "nonsuppressed", "oratorize", "uncarefully",
			"benzothiofuran", "penult", "balanocele", "macropterous",
			"dishpan", "marten", "absvolt", "jirble", "parmelioid",
			"airfreighter", "acocotl", "archesporial", "hypoplastral",
			"preoral", "quailberry", "cinque", "terrestrially", "stroking",
			"limpet", "moodishness", "canicule", "archididascalian",
			"pompiloid", "overstaid", "introducer", "Italical",
			"Christianopaganism", "prescriptible", "subofficer", "danseuse",
			"cloy", "saguran", "frictionlessly", "deindividualization",
			"Bulanda", "ventricous", "subfoliar", "basto", "scapuloradial",
			"suspend", "stiffish", "Sphenodontidae", "eternal", "verbid",
			"mammonish", "upcushion", "barkometer", "concretion", "preagitate",
			"incomprehensible", "tristich", "visceral", "hemimelus",
			"patroller", "stentorophonic", "pinulus", "kerykeion", "brutism",
			"monstership", "merciful", "overinstruct", "defensibly",
			"bettermost", "splenauxe", "Mormyrus", "unreprimanded", "taver",
			"ell", "proacquittal", "infestation", "overwoven", "Lincolnlike",
			"chacona", "Tamil", "classificational", "lebensraum", "reeveland",
			"intuition", "Whilkut", "focaloid", "Eleusinian", "micromembrane",
			"byroad", "nonrepetition", "bacterioblast", "brag", "ribaldrous",
			"phytoma", "counteralliance", "pelvimetry", "pelf", "relaster",
			"thermoresistant", "aneurism", "molossic", "euphonym", "upswell",
			"ladhood", "phallaceous", "inertly", "gunshop", "stereotypography",
			"laryngic", "refasten", "twinling", "oflete", "hepatorrhaphy",
			"electrotechnics", "cockal", "guitarist", "topsail",
			"Cimmerianism", "larklike", "Llandovery", "pyrocatechol",
			"immatchable", "chooser", "metrocratic", "craglike", "quadrennial",
			"nonpoisonous", "undercolored", "knob", "ultratense",
			"balladmonger", "slait", "sialadenitis", "bucketer",
			"magnificently", "unstipulated", "unscourged", "unsupercilious",
			"packsack", "pansophism", "soorkee", "percent", "subirrigate",
			"champer", "metapolitics", "spherulitic", "involatile",
			"metaphonical", "stachyuraceous", "speckedness", "bespin",
			"proboscidiform", "gul", "squit", "yeelaman", "peristeropode",
			"opacousness", "shibuichi", "retinize", "yote", "misexposition",
			"devilwise", "pumpkinification", "vinny", "bonze", "glossing",
			"decardinalize", "transcortical", "serphoid", "deepmost",
			"guanajuatite", "wemless", "arval", "lammy", "Effie", "Saponaria",
			"tetrahedral", "prolificy", "excerpt", "dunkadoo", "Spencerism",
			"insatiately", "Gilaki", "oratorship", "arduousness",
			"unbashfulness", "Pithecolobium", "unisexuality", "veterinarian",
			"detractive", "liquidity", "acidophile", "proauction", "sural",
			"totaquina", "Vichyite", "uninhabitedness", "allegedly", "Gothish",
			"manny", "Inger", "flutist", "ticktick", "Ludgatian",
			"homotransplant", "orthopedical", "diminutively", "monogoneutic",
			"Kenipsim", "sarcologist", "drome", "stronghearted", "Fameuse",
			"Swaziland", "alen", "chilblain", "beatable", "agglomeratic",
			"constitutor", "tendomucoid", "porencephalous", "arteriasis",
			"boser", "tantivy", "rede", "lineamental", "uncontradictableness",
			"homeotypical", "masa", "folious", "dosseret", "neurodegenerative",
			"subtransverse", "Chiasmodontidae", "palaeotheriodont",
			"unstressedly", "chalcites", "piquantness", "lampyrine",
			"Aplacentalia", "projecting", "elastivity", "isopelletierin",
			"bladderwort", "strander", "almud", "iniquitously", "theologal",
			"bugre", "chargeably", "imperceptivity", "meriquinoidal",
			"mesophyte", "divinator", "perfunctory", "counterappellant",
			"synovial", "charioteer", "crystallographical", "comprovincial",
			"infrastapedial", "pleasurehood", "inventurous", "ultrasystematic",
			"subangulated", "supraoesophageal", "Vaishnavism", "transude",
			"chrysochrous", "ungrave", "reconciliable", "uninterpleaded",
			"erlking", "wherefrom", "aprosopia", "antiadiaphorist",
			"metoxazine", "incalculable", "umbellic", "predebit", "foursquare",
			"unimmortal", "nonmanufacture", "slangy", "predisputant",
			"familist", "preaffiliate", "friarhood", "corelysis", "zoonitic",
			"halloo", "paunchy", "neuromimesis", "aconitine", "hackneyed",
			"unfeeble", "cubby", "autoschediastical", "naprapath", "lyrebird",
			"inexistency", "leucophoenicite", "ferrogoslarite", "reperuse",
			"uncombable", "tambo", "propodiale", "diplomatize", "Russifier",
			"clanned", "corona", "michigan", "nonutilitarian",
			"transcorporeal", "bought", "Cercosporella", "stapedius",
			"glandularly", "pictorially", "weism", "disilane", "rainproof",
			"Caphtor", "scrubbed", "oinomancy", "pseudoxanthine",
			"nonlustrous", "redesertion", "Oryzorictinae", "gala", "Mycogone",
			"reappreciate", "cyanoguanidine", "seeingness", "breadwinner",
			"noreast", "furacious", "epauliere", "omniscribent",
			"Passiflorales", "uninductive", "inductivity", "Orbitolina",
			"Semecarpus", "migrainoid", "steprelationship", "phlogisticate",
			"mesymnion", "sloped", "edificator", "beneficent", "culm",
			"paleornithology", "unurban", "throbless", "amplexifoliate",
			"sesquiquintile", "sapience", "astucious", "dithery", "boor",
			"ambitus", "scotching", "uloid", "uncompromisingness", "hoove",
			"waird", "marshiness", "Jerusalem", "mericarp", "unevoked",
			"benzoperoxide", "outguess", "pyxie", "hymnic", "euphemize",
			"mendacity", "erythremia", "rosaniline", "unchatteled",
			"lienteria", "Bushongo", "dialoguer", "unrepealably", "rivethead",
			"antideflation", "vinegarish", "manganosiderite", "doubtingness",
			"ovopyriform", "Cephalodiscus", "Muscicapa", "Animalivora",
			"angina", "planispheric", "ipomoein", "cuproiodargyrite",
			"sandbox", "scrat", "Munnopsidae", "shola", "pentafid",
			"overstudiousness", "times", "nonprofession", "appetible",
			"valvulotomy", "goladar", "uniarticular", "oxyterpene",
			"unlapsing", "omega", "trophonema", "seminonflammable",
			"circumzenithal", "starer", "depthwise", "liberatress",
			"unleavened", "unrevolting", "groundneedle", "topline", "wandoo",
			"umangite", "ordinant", "unachievable", "oversand", "snare",
			"avengeful", "unexplicit", "mustafina", "sonable",
			"rehabilitative", "eulogization", "papery", "technopsychology",
			"impressor", "cresylite", "entame", "transudatory", "scotale",
			"pachydermatoid", "imaginary", "yeat", "slipped", "stewardship",
			"adatom", "cockstone", "skyshine", "heavenful", "comparability",
			"exprobratory", "dermorhynchous", "parquet", "cretaceous",
			"vesperal", "raphis", "undangered", "Glecoma", "engrain",
			"counteractively", "Zuludom", "orchiocatabasis", "Auriculariales",
			"warriorwise", "extraorganismal", "overbuilt", "alveolite",
			"tetchy", "terrificness", "widdle", "unpremonished", "rebilling",
			"sequestrum", "equiconvex", "heliocentricism", "catabaptist",
			"okonite", "propheticism", "helminthagogic", "calycular",
			"giantly", "wingable", "golem", "unprovided", "commandingness",
			"greave", "haply", "doina", "depressingly", "subdentate",
			"impairment", "decidable", "neurotrophic", "unpredict",
			"bicorporeal", "pendulant", "flatman", "intrabred", "toplike",
			"Prosobranchiata", "farrantly", "toxoplasmosis", "gorilloid",
			"dipsomaniacal", "aquiline", "atlantite", "ascitic", "perculsive",
			"prospectiveness", "saponaceous", "centrifugalization", "dinical",
			"infravaginal", "beadroll", "affaite", "Helvidian", "tickleproof",
			"abstractionism", "enhedge", "outwealth", "overcontribute",
			"coldfinch", "gymnastic", "Pincian", "Munychian", "codisjunct",
			"quad", "coracomandibular", "phoenicochroite", "amender",
			"selectivity", "putative", "semantician", "lophotrichic",
			"Spatangoidea", "saccharogenic", "inferent", "Triconodonta",
			"arrendation", "sheepskin", "taurocolla", "bunghole", "Machiavel",
			"triakistetrahedral", "dehairer", "prezygapophysial", "cylindric",
			"pneumonalgia", "sleigher", "emir", "Socraticism", "licitness",
			"massedly", "instructiveness", "sturdied", "redecrease",
			"starosta", "evictor", "orgiastic", "squdge", "meloplasty",
			"Tsonecan", "repealableness", "swoony", "myesthesia", "molecule",
			"autobiographist", "reciprocation", "refective", "unobservantness",
			"tricae", "ungouged", "floatability", "Mesua", "fetlocked",
			"chordacentrum", "sedentariness", "various", "laubanite",
			"nectopod", "zenick", "sequentially", "analgic", "biodynamics",
			"posttraumatic", "nummi", "pyroacetic", "bot", "redescend",
			"dispermy", "undiffusive", "circular", "trillion", "Uraniidae",
			"ploration", "discipular", "potentness", "sud", "Hu", "Eryon",
			"plugger", "subdrainage", "jharal", "abscission", "supermarket",
			"countergabion", "glacierist", "lithotresis", "minniebush",
			"zanyism", "eucalypteol", "sterilely", "unrealize", "unpatched",
			"hypochondriacism", "critically", "cheesecutter" };

	static int printUsage() {
		System.out
				.println("randomtextwriter [-outFormat <output format class>] <output>");

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception {
		if (args.length == 0) {
			return printUsage();
		}

		JobConf job = new JobConf(getConf());

		job.setJarByClass(RandomTextWriter.class);
		job.setJobName("random-text-writer");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormat(RandomWriter.RandomInputFormat.class);
		job.setMapperClass(Map.class);

		JobClient client = new JobClient(job);
		ClusterStatus cluster = client.getClusterStatus();
		int numMapsPerHost = job.getInt("test.randomtextwrite.maps_per_host",
				10);
		long numBytesToWritePerMap = job.getLong(
				"test.randomtextwrite.bytes_per_map", 1073741824L);

		if (numBytesToWritePerMap == 0L) {
			System.err
					.println("Cannot have test.randomtextwrite.bytes_per_map set to 0");
			return -2;
		}
		long totalBytesToWrite = job.getLong(
				"test.randomtextwrite.total_bytes", numMapsPerHost
						* numBytesToWritePerMap * cluster.getTaskTrackers());

		int numMaps = (int) (totalBytesToWrite / numBytesToWritePerMap);
		if ((numMaps == 0) && (totalBytesToWrite > 0L)) {
			numMaps = 1;
			job.setLong("test.randomtextwrite.bytes_per_map", totalBytesToWrite);
		}

		Class outputFormatClass = SequenceFileOutputFormat.class;

		List otherArgs = new ArrayList();
		for (int i = 0; i < args.length; i++) {
			try {
				if ("-outFormat".equals(args[i])) {
					outputFormatClass = Class.forName(args[(++i)]).asSubclass(
							OutputFormat.class);
				} else
					otherArgs.add(args[i]);
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from "
						+ args[(i - 1)]);

				return printUsage();
			}
		}

		job.setOutputFormat(outputFormatClass);
		FileOutputFormat
				.setOutputPath(job, new Path((String) otherArgs.get(0)));

		job.setNumMapTasks(numMaps);
		System.out.println("Running " + numMaps + " maps.");

		job.setNumReduceTasks(0);

		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		JobClient.runJob(job);
		Date endTime = new Date();
		System.out.println("Job ended: " + endTime);
		System.out.println("The job took "
				+ (endTime.getTime() - startTime.getTime()) / 1000L
				+ " seconds.");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RandomTextWriter(),
				args);
		System.exit(res);
	}

	static class Map extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {
		private long numBytesToWrite;
		private int minWordsInKey;
		private int wordsInKeyRange;
		private int minWordsInValue;
		private int wordsInValueRange;
		private Random random = new Random();

		public void configure(JobConf job) {
			this.numBytesToWrite = job.getLong(
					"test.randomtextwrite.bytes_per_map", 1073741824L);

			this.minWordsInKey = job.getInt(
					"test.randomtextwrite.min_words_key", 5);

			this.wordsInKeyRange = (job.getInt(
					"test.randomtextwrite.max_words_key", 10) - this.minWordsInKey);

			this.minWordsInValue = job.getInt(
					"test.randomtextwrite.min_words_value", 10);

			this.wordsInValueRange = (job.getInt(
					"test.randomtextwrite.max_words_value", 100) - this.minWordsInValue);
		}

		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int itemCount = 0;
			while (this.numBytesToWrite > 0L) {
				int noWordsKey = this.minWordsInKey
						+ (this.wordsInKeyRange != 0 ? this.random
								.nextInt(this.wordsInKeyRange) : 0);

				int noWordsValue = this.minWordsInValue
						+ (this.wordsInValueRange != 0 ? this.random
								.nextInt(this.wordsInValueRange) : 0);

				Text keyWords = generateSentence(noWordsKey);
				Text valueWords = generateSentence(noWordsValue);

				output.collect(keyWords, valueWords);

				this.numBytesToWrite -= keyWords.getLength()
						+ valueWords.getLength();

				reporter.incrCounter(RandomTextWriter.Counters.BYTES_WRITTEN,
						keyWords.getLength() + valueWords.getLength());

				reporter.incrCounter(RandomTextWriter.Counters.RECORDS_WRITTEN,
						1L);
				itemCount++;
				if (itemCount % 200 == 0) {
					reporter.setStatus("wrote record " + itemCount + ". "
							+ this.numBytesToWrite + " bytes left.");
				}
			}

			reporter.setStatus("done with " + itemCount + " records.");
		}

		private Text generateSentence(int noWords) {
			StringBuffer sentence = new StringBuffer();
			String space = " ";
			for (int i = 0; i < noWords; i++) {
				sentence.append(RandomTextWriter.words[this.random
						.nextInt(RandomTextWriter.words.length)]);
				sentence.append(space);
			}
			return new Text(sentence.toString());
		}
	}

	static enum Counters {
		RECORDS_WRITTEN, BYTES_WRITTEN;
	}
}


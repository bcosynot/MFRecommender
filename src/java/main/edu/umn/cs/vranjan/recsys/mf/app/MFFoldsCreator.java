package edu.umn.cs.vranjan.recsys.mf.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;

import org.grouplens.lenskit.GlobalItemScorer;
import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.ItemScorer;
import org.grouplens.lenskit.RecommenderBuildException;
import org.grouplens.lenskit.baseline.BaselineScorer;
import org.grouplens.lenskit.baseline.ItemMeanRatingItemScorer;
import org.grouplens.lenskit.basic.TopNItemRecommender;
import org.grouplens.lenskit.core.LenskitConfiguration;
import org.grouplens.lenskit.core.LenskitRecommender;
import org.grouplens.lenskit.data.dao.EventDAO;
import org.grouplens.lenskit.data.dao.PrefetchingItemDAO;
import org.grouplens.lenskit.data.dao.PrefetchingUserDAO;
import org.grouplens.lenskit.data.dao.PrefetchingUserEventDAO;
import org.grouplens.lenskit.data.dao.SimpleFileRatingDAO;
import org.grouplens.lenskit.data.pref.PreferenceDomain;
import org.grouplens.lenskit.eval.EvalConfig;
import org.grouplens.lenskit.eval.EvalProject;
import org.grouplens.lenskit.eval.TaskExecutionException;
import org.grouplens.lenskit.eval.algorithm.AlgorithmInstanceBuilder;
import org.grouplens.lenskit.eval.data.CSVDataSourceBuilder;
import org.grouplens.lenskit.eval.data.DataSource;
import org.grouplens.lenskit.eval.data.crossfold.CrossfoldMethod;
import org.grouplens.lenskit.eval.data.crossfold.CrossfoldTask;
import org.grouplens.lenskit.eval.data.traintest.TTDataSet;
import org.grouplens.lenskit.eval.metrics.predict.CoveragePredictMetric;
import org.grouplens.lenskit.eval.metrics.predict.NDCGPredictMetric;
import org.grouplens.lenskit.eval.metrics.predict.RMSEPredictMetric;
import org.grouplens.lenskit.eval.metrics.topn.IndependentRecallTopNMetric;
import org.grouplens.lenskit.eval.metrics.topn.ItemSelectors;
import org.grouplens.lenskit.eval.metrics.topn.NDCGTopNMetric;
import org.grouplens.lenskit.eval.traintest.SimpleEvaluator;
import org.grouplens.lenskit.knn.item.ItemItemGlobalScorer;
import org.grouplens.lenskit.mf.funksvd.FunkSVDItemScorer;
import org.grouplens.lenskit.mf.funksvd.FunkSVDModel;
import org.grouplens.lenskit.mf.funksvd.FunkSVDUpdateRule;
import org.grouplens.lenskit.scored.ScoredId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MFFoldsCreator {

	private static Logger logger = LoggerFactory
			.getLogger(MFFoldsCreator.class);

	private static String ORIGINAL_FILE_NAME = "user_artists.dat";
	private static String ORIGINAL_FILE_DELIMITER = "\t";
	private static String RATINGS_FILE_NAME = "user_artists_ratings.dat";
	private static String RATINGS_FILE_DELIMITER = "\t";
	private static Integer[] RATINGS_FILE_RATINGS_RANGE = { 1, 5 };

	public static void main(String args[]) {

		logger.info("Creating normalized ratings file..");
		// Read original file
		BufferedReader br = null;
		BufferedWriter bw = null;
		Long startTime = System.currentTimeMillis();
		int userWithMaxMappings = -99;
		int maxMappings = -99;
		int noOfRecommendations = 50;

		try {
			br = new BufferedReader(new FileReader(ORIGINAL_FILE_NAME));
			bw = new BufferedWriter(new FileWriter(RATINGS_FILE_NAME));
			Integer ratingsRangeMin = RATINGS_FILE_RATINGS_RANGE[0];
			Integer ratingsRangeMax = RATINGS_FILE_RATINGS_RANGE[1];
			Iterator<String> lines = br.lines().iterator();
			int previousUserId = -99;
			int currentUserId = -99;
			int userCount = 0;
			int userArtistsCount = 0;
			Map<Long, Long> artistsAndWeights = new HashMap<Long, Long>();
			// Read weights and write normalized ratings.
			while (lines.hasNext()) {
				StringTokenizer st = new StringTokenizer(lines.next(),
						ORIGINAL_FILE_DELIMITER);
				currentUserId = Integer.parseInt(st.nextToken().trim());
				// If user is not the same, generate and write normalized
				// ratings.
				if (previousUserId != -99 && previousUserId != currentUserId) {
					normalizeAndWriteRatings(previousUserId, artistsAndWeights,
							bw, ratingsRangeMin, ratingsRangeMax);
					if (maxMappings < artistsAndWeights.size()) {
						userWithMaxMappings = previousUserId;
						maxMappings = artistsAndWeights.size();
					}
					artistsAndWeights = new HashMap<Long, Long>();
					userCount++;
				}
				// Add the user's artist and weight mapping.
				artistsAndWeights.put(Long.parseLong(st.nextToken().trim()),
						Long.parseLong(st.nextToken().trim()));
				previousUserId = currentUserId;
				userArtistsCount++;
			}
			// Write for last user.
			normalizeAndWriteRatings(previousUserId, artistsAndWeights, bw,
					ratingsRangeMin, ratingsRangeMax);
			logger.info("Total users: " + (++userCount));
			logger.info("Total user-artists mappings: " + userArtistsCount);
		} catch (FileNotFoundException e) {
			logger.error("Original file with weights not found.", e);
		} catch (IOException e) {
			logger.error("Error while writing file.", e);
		} finally {
			if (null != br) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error("Error while closing reader.", e);
				}
			}
			if (null != bw) {
				try {
					bw.flush();
				} catch (IOException e) {
					logger.error("Error while flushing file.", e);
				}
				try {
					bw.close();
				} catch (IOException e) {
					logger.error("Error while closing writer.", e);
				}
			}
		}
		logger.info("Time taken to write normalized ratings file: "
				+ (System.currentTimeMillis() - startTime) / 1000 + "s");

		// Configure lenskit
		AlgorithmInstanceBuilder algoBuilder = new AlgorithmInstanceBuilder(
				"FunkSVD");

		LenskitConfiguration lenskitConfig = algoBuilder.getConfig();
		setupLenskitConfiguration(lenskitConfig);

		// Get recommendations.
		getRecommendations(lenskitConfig, userWithMaxMappings,
				noOfRecommendations, algoBuilder);

		// Run the evaluator
		runEvaluations(algoBuilder);

	}

	/**
	 * @param lenskitConfig
	 * @param userId
	 * @param noOfRecommendations
	 * @param algoBuilder
	 */
	private static void getRecommendations(LenskitConfiguration lenskitConfig,
			int userId, int noOfRecommendations,
			AlgorithmInstanceBuilder algoBuilder) {

		// Run the recommendation engine
		try {
			LenskitRecommender recommender = LenskitRecommender
					.build(lenskitConfig);
			TopNItemRecommender itemRecommender = (TopNItemRecommender) recommender
					.getItemRecommender();
			logger.info("Fetching " + noOfRecommendations
					+ " recommendations for user " + userId);
			List<ScoredId> recommendations = itemRecommender.recommend(
					userId, noOfRecommendations);
			for (ScoredId recommendation : recommendations) {
				logger.info("Recommendation : " + recommendation.getId()
						+ " - " + recommendation.getScore());
			}
		} catch (RecommenderBuildException e) {
			logger.error("Error while building recommender.", e);
		}
	}

	/**
	 * @param algoBuilder
	 */
	private static void runEvaluations(AlgorithmInstanceBuilder algoBuilder) {

		Properties properties = new Properties();
		logger.info("Running the evaluator..");
		properties.setProperty(EvalConfig.THREAD_COUNT_PROPERTY, "20");
		SimpleEvaluator simpleEval = new SimpleEvaluator(properties);
		EvalProject project = simpleEval.getRawCommand().getProject();
		algoBuilder.setProject(project);
		simpleEval.addAlgorithm(algoBuilder.build());
		CSVDataSourceBuilder csvDataSourceBuilder = new CSVDataSourceBuilder(
				new File(RATINGS_FILE_NAME));
		csvDataSourceBuilder.setDelimiter(RATINGS_FILE_DELIMITER);
		csvDataSourceBuilder.setName("UsersAndArtistsRatings");
		DataSource source = csvDataSourceBuilder.build();
		CrossfoldTask crossFoldTask = new CrossfoldTask();
		if (null != simpleEval.getRawCommand()) {
			if (null != project) {
				logger.debug("Setting project");
				crossFoldTask.setProject(project);
			}
		}
		crossFoldTask.setHoldoutFraction(0.1f);
		crossFoldTask.setMethod(CrossfoldMethod.PARTITION_USERS);
		crossFoldTask.setPartitions(2);
		crossFoldTask.setSource(source);
		try {
			for (TTDataSet dataSet : crossFoldTask.perform()) {
				simpleEval.addDataset(dataSet);
			}
		} catch (TaskExecutionException e1) {
			e1.printStackTrace();
		}
		NDCGTopNMetric ndcgTopNMetric = new NDCGTopNMetric(null, null, 50,
				ItemSelectors.allItems(), ItemSelectors.trainingItems());
		simpleEval.addMetric(ndcgTopNMetric);
		IndependentRecallTopNMetric independentRecallTopNMetric = new IndependentRecallTopNMetric(
				null, null, ItemSelectors.allItems(), ItemSelectors.allItems(),
				10, ItemSelectors.trainingItems());
		simpleEval.addMetric(independentRecallTopNMetric);
		simpleEval.addMetric(NDCGPredictMetric.class);
		simpleEval.addMetric(RMSEPredictMetric.class);
		simpleEval.addMetric(CoveragePredictMetric.class);

		simpleEval.setOutput(new File("eval-results.csv"));
		try {
			simpleEval.call();
		} catch (TaskExecutionException e) {
			logger.error("Error while running evaluations.", e);
		}
		logger.info("Evaluations over.");
	}

	private static void setupLenskitConfiguration(
			LenskitConfiguration lenskitConfig) {

		// Configure the ratings range.
		PreferenceDomain preferenceDomain = new PreferenceDomain(
				RATINGS_FILE_RATINGS_RANGE[0].doubleValue(),
				RATINGS_FILE_RATINGS_RANGE[1].doubleValue());
		lenskitConfig.bind(PreferenceDomain.class).to(preferenceDomain);

		// Configure the EventDAO
		final File inputFile = new File(RATINGS_FILE_NAME);
		SimpleFileRatingDAO simpleFileRatingDAO = new SimpleFileRatingDAO(
				inputFile, RATINGS_FILE_DELIMITER);
		lenskitConfig.bind(EventDAO.class).to(simpleFileRatingDAO);

		// Configure the user event DAO
		lenskitConfig.addComponent(PrefetchingUserEventDAO.class);

		// Configure the item DAO
		lenskitConfig.addComponent(PrefetchingItemDAO.class);

		// Configure the user DAO
		lenskitConfig.addComponent(PrefetchingUserDAO.class);

		// Configure the recommender
		lenskitConfig.bind(ItemRecommender.class).to(TopNItemRecommender.class);

		// Configure the item scorer
		lenskitConfig.addComponent(FunkSVDItemScorer.class);

		// Configure the baseline scorer.
		lenskitConfig.bind(BaselineScorer.class, ItemScorer.class).to(
				ItemMeanRatingItemScorer.class);

		// Configure the global item scorer
		lenskitConfig.bind(GlobalItemScorer.class).to(
				ItemItemGlobalScorer.class);

		// Configure the update rule
		lenskitConfig.addComponent(FunkSVDUpdateRule.class);

		// Configure the model
		lenskitConfig.addComponent(FunkSVDModel.class);

	}

	private static void normalizeAndWriteRatings(Integer userId,
			Map<Long, Long> artistsAndWeights, BufferedWriter bw,
			Integer ratingsRangeMin, Integer ratingsRangeMax) {

		Collection<Long> weights = artistsAndWeights.values();
		Long max = Collections.max(weights);
		Long min = Collections.min(weights);

		Map<Long, Integer> artistsAndRatings = new HashMap<Long, Integer>();
		for (Entry<Long, Long> e : artistsAndWeights.entrySet()) {
			Long weight = e.getValue();
			Long artistId = e.getKey();
			String parameters = "";
			if (min != max) {
				try {
					parameters = min + " - " + max + ":" + weight;
					Long normalizedRating = ((Long) ((ratingsRangeMax - ratingsRangeMin) * (weight - min))
							/ (max - min) + ratingsRangeMin);
					parameters = parameters + " - " + normalizedRating;
					logger.trace(parameters);
					artistsAndRatings
							.put(artistId, normalizedRating.intValue());
				} catch (Exception e1) {
					logger.error(
							"Error while calculating normalized rating for user "
									+ " userId" + userId + " and artist"
									+ artistId
									+ " with the following paramaters "
									+ parameters, e1);
				}
			} else {
				artistsAndRatings.put(artistId,
						(ratingsRangeMax + ratingsRangeMin) / 2);
			}
		}

		for (Entry<Long, Integer> e : artistsAndRatings.entrySet()) {
			StringBuilder line = new StringBuilder();
			line.append(userId).append(RATINGS_FILE_DELIMITER)
					.append(e.getKey()).append(RATINGS_FILE_DELIMITER)
					.append(e.getValue()).append("\n");
			try {
				bw.write(line.toString());
			} catch (IOException e1) {
				logger.error("Error while writing line : " + line.toString(), e);
			}
		}

	}
}

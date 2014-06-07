package edu.umn.cs.vranjan.recsys.mf.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

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
			try {
				parameters = min + " - " + max + ":" + weight;
				Long normalizedRating = ((Long) ((ratingsRangeMax - ratingsRangeMin) * (weight - min))
						/ (max - min) + ratingsRangeMin);
				parameters = parameters + " - " + normalizedRating;
				logger.trace(parameters);
				artistsAndRatings.put(artistId, normalizedRating.intValue());
			} catch (Exception e1) {
				logger.error(
						"Error while calculating normalized rating for user "
								+ " userId" + userId + " and artist" + artistId
								+ " with the following paramaters "
								+ parameters, e1);
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

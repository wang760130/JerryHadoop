package com.jerry.mahout.collaborativefiltering;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/**
 * 协同过滤userCF
 * data:
 * 	1,101,5.0
 * 	用户ID，物品ID，用户对物品的打分。
 * @author Wangjiajun 
 * @Email  wangjiajun@58.com
 * @date   2015年7月27日
 */
public class UserCF {
	private final static int NEIGHBORHOOD_NUM = 2;
	private final static int RECOMMENDER_NUM = 3;
	
	private final static String FILE = "datafile/item.txt";

	public static void main(String[] args) throws IOException, TasteException {
		File file = new File(FILE);
		DataModel model = new FileDataModel(file);
		UserSimilarity user = new EuclideanDistanceSimilarity(model);
		NearestNUserNeighborhood neighbor = new NearestNUserNeighborhood(
				NEIGHBORHOOD_NUM, user, model);
		Recommender r = new GenericUserBasedRecommender(model, neighbor, user);
		LongPrimitiveIterator iter = model.getUserIDs();

		while (iter.hasNext()) {
			long uid = iter.nextLong();
			List<RecommendedItem> list = r.recommend(uid, RECOMMENDER_NUM);
			System.out.printf("uid:%s", uid);
			for (RecommendedItem ritem : list) {
				System.out.printf("(%s,%f)", ritem.getItemID(),ritem.getValue());
			}
			System.out.println();
		}
	}
}

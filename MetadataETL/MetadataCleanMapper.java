import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MetadataCleanMapper
	extends Mapper<Object, Text, Text, Text> {
	
	@Override
	public void map(Object key, Text value, Context context) 
	   throws IOException, InterruptedException {

	   		String s = value.toString();
			s = s.replaceAll(", ", " ");
			String[] strs = s.split(",");


			String imdb_score = "";

			String num_critic_for_reviews = "";
			String duration = "";
			String director_facebook_likes = "";
			String actor_3_facebook_likes = "";
			String actor_1_facebook_likes = "";
			String gross = "";
			String num_voted_users = "";
			String cast_total_facebook_likes = "";
			String facenumber_in_poster = "";
			String num_user_for_reviews = "";
			String budget = "";
			String title_year = "";
			String actor_2_facebook_likes = "";
			String aspect_ratio = "";
			String movie_facebook_likes = "";

			
			if(strs.length > 27)
			{
				imdb_score = strs[25];

				num_critic_for_reviews = strs[2];
				duration = strs[3];
				director_facebook_likes = strs[4];
				actor_3_facebook_likes = strs[5];
				actor_1_facebook_likes = strs[7];
				gross = strs[8];
				num_voted_users = strs[12];
				cast_total_facebook_likes = strs[13];
				facenumber_in_poster = strs[15];
				num_user_for_reviews = strs[18];
				budget = strs[22];
				title_year = strs[23];
				actor_2_facebook_likes = strs[24];
				aspect_ratio = strs[26];
				movie_facebook_likes = strs[27];
				
				StringBuffer sb = new StringBuffer("");
				
				//num_critic_for_reviews: 1
				if(num_critic_for_reviews.matches("^-?\\d+$"))
				{
					sb.append(" 1:"+ num_critic_for_reviews + " ");  //remember to keep the space before 1
				}
				else
				{
					sb.append(" ");
				}
				
				//duration: 2
				if(duration.matches("^-?\\d+$")) 
				{
					sb.append("2:" + duration + " ");
				}
				else
				{
					sb.append(" ");
				}
				
				//director_facebook_likes: 3
				if(director_facebook_likes.matches("^-?\\d+$"))
				{
					sb.append("3:" + director_facebook_likes + " ");
				}
				else
				{
					sb.append(" ");
				}
				
				//actor_3_facebook_likes: 4
				if(actor_3_facebook_likes.matches("^-?\\d+$")) 
				{
					sb.append("4:" + actor_3_facebook_likes+ " ");
				}
				else
				{
					sb.append(" ");
				}

				//actor_1_facebook_likes: 5
				if(actor_1_facebook_likes.matches("^-?\\d+$"))
				{
					sb.append(" 5:"+ actor_1_facebook_likes + " "); 
				}
				else
				{
					sb.append(" ");
				}

				//gross: 6
				if(gross.matches("^-?\\d+$"))
				{
					sb.append(" 6:"+ gross + " ");  
				}
				else
				{
					sb.append(" ");
				}

				//num_voted_users: 7
				if(num_voted_users.matches("^-?\\d+$"))
				{
					sb.append(" 7:"+ num_voted_users + " "); 
				}
				else
				{
					sb.append(" ");
				}

				//cast_total_facebook_likes: 8
				if(cast_total_facebook_likes.matches("^-?\\d+$"))
				{
					sb.append(" 8:"+ cast_total_facebook_likes + " ");  
				}
				else
				{
					sb.append(" ");
				}

				//facenumber_in_poster: 9
				if(facenumber_in_poster.matches("^-?\\d+$"))
				{
					sb.append(" 9:"+ facenumber_in_poster + " ");  
				}
				else
				{
					sb.append(" ");
				}

				//num_user_for_reviews: 10
				if(num_user_for_reviews.matches("^-?\\d+$"))
				{
					sb.append(" 10:"+ num_user_for_reviews + " ");  
				}
				else
				{
					sb.append(" ");
				}

				//budget: 11
				if(budget.matches("^-?\\d+$"))
				{
					sb.append(" 11:"+ budget + " ");  
				}
				else
				{
					sb.append(" ");
				}

				//title_year: 12
				if(title_year.matches("^-?\\d+$"))
				{
					sb.append(" 12:"+ title_year + " ");  
				}
				else
				{
					sb.append(" ");
				}

				//actor_2_facebook_likes: 13
				if(actor_2_facebook_likes.matches("^-?\\d+$"))
				{
					sb.append(" 13:"+ actor_2_facebook_likes + " "); 
				}
				else
				{
					sb.append(" ");
				}

				//aspect_ratio: 14
				if(aspect_ratio.matches("([0-9]*)\\.([0-9]*)"))
				{
					sb.append(" 14:"+ aspect_ratio + " ");  
				}
				else
				{
					sb.append(" ");
				}

				//movie_facebook_likes: 15
				if(movie_facebook_likes.matches("^-?\\d+$"))
				{
					sb.append(" 15:"+ movie_facebook_likes + " ");  
				}
				else
				{
					sb.append(" ");
				}



				
				if(imdb_score.matches("([0-9]*)\\.([0-9]*)") && Double.parseDouble(imdb_score) <= 10.0D && Double.parseDouble(imdb_score) >= 0.0D) 
				{
					context.write(new Text(imdb_score), new Text(sb.toString()));
				}
			}

		}
}
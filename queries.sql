--- Movie(s) with the highest average rating 

SELECT
  r.movie_id,
  AVG(r.rating) AS avg_rating,
  COUNT(*) AS n_ratings
FROM ratings r
WHERE r.rating IS NOT NULL
GROUP BY r.movie_id
HAVING AVG(r.rating) = (
  SELECT MAX(avg_rating)
  FROM (
    SELECT AVG(rating) AS avg_rating
    FROM ratings
    WHERE rating IS NOT NULL
    GROUP BY movie_id
  ) AS sub
);


--- Director with the most movies in the dataset

SELECT
  od.director,
  COUNT(*) AS n_movies
FROM omdb_details od
JOIN movies m ON m.movie_id = od.movie_id
WHERE od.director IS NOT NULL AND od.director <> ''
GROUP BY od.director
ORDER BY n_movies DESC
LIMIT 1;


--- Top 5 genres with the highest average rating

SELECT
  mg.genre,
  AVG(r.rating) AS avg_rating,
  COUNT(*) AS n_ratings
FROM ratings r
JOIN movie_genres mg ON mg.movie_id = r.movie_id
GROUP BY mg.genre
ORDER BY avg_rating DESC
LIMIT 5;


--- Average rating of movies released each year

SELECT
  m.year,
  AVG(r.rating) AS avg_rating,
  COUNT(*) AS n_ratings
FROM ratings r
JOIN movies m ON m.movie_id = r.movie_id
WHERE m.year IS NOT NULL
GROUP BY m.year
ORDER BY m.year;

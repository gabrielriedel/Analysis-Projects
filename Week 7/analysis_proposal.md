# Analysis Proposal

1. The question I am trying to answer is, is there a launch angle (or range of launch angles) for baseball hitters to generate for optimal outcomes?
2. I love baseball, and my dream job is to work for a professional baseball team doing data science. I want to answer this specific question because the answer is practical in the sense that a player can adjust and work on their swing to reach the optimal launch angle.  
3. My hypothesis is that the optimal launch angle will be around 45 degrees. Theoretically, the most ideal result of an at bat is a home run, and most home runs are flyballs which need to have a high launch angle to take their parabolic shape. However, the angle cannot be too high otherwise the ball will not travel far enough in horizontal distance. 
4. My data will be loaded in using a package in python called pybaseball. This packages essentially provides an api to access play-by-play data for over a century worth of Major League Baseball seasons. I have not loaded it in fully yet, but I will load in enough seasons worth of data to be over 15gb when I write it to a csv (one season
 is approximately 500mb worth of data). Here is a link to the package: https://github.com/jldbc/pybaseball

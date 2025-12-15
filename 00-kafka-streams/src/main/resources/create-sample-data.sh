#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=========================================${NC}"
echo -e "${YELLOW}  Kafka Sample Data Generator           ${NC}"
echo -e "${YELLOW}=========================================${NC}"

# Check if kafka-console-producer is available
if ! command -v kafka-console-producer &> /dev/null; then
    echo -e "${RED}Error: kafka-console-producer not found in PATH${NC}"
    echo "Make sure Kafka binaries are installed and in your PATH"
    exit 1
fi

# Kafka broker configuration
BOOTSTRAP_SERVER="localhost:9092"
TOPICS=(
    "star-wars-quotes"
    "disney-quotes"
    "arnold-swerthenagger-quotes"
    "basketball-quotes"
    "basketball-teams"
    "basketball-players"
    "basketball-games"
    "basketball-scores"
)

# Create topics if they don't exist (optional)
echo -e "\n${GREEN}Creating topics...${NC}"
for topic in "${TOPICS[@]}"; do
    echo "Creating topic: $topic"
    kafka-topics --create --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $topic \
        --partitions 1 \
        --replication-factor 1 2>/dev/null || echo "Topic $topic already exists or error occurred"
done

echo -e "\n${GREEN}Generating sample data...${NC}"

# Generate Star Wars quotes data
echo -e "\n${YELLOW}--- Sending Star Wars Quotes ---${NC}"
cat <<EOF | kafka-console-producer --bootstrap-server $BOOTSTRAP_SERVER --topic star-wars-quotes --property "parse.key=true" --property "key.separator=:"
luke1:"May the Force be with you."
vader1:"I am your father."
yoda1:"Do or do not. There is no try."
obiwan1:"The Force will be with you, always."
han1:"Never tell me the odds!"
luke2:"I've got a bad feeling about this."
obiwan2:"These are not the droids you're looking for."
ackbar1:"It's a trap!"
palpatine1:"The dark side of the Force is a pathway to many abilities some consider to be unnatural."
obiwan3:"Hello there!"
yoda2:"Fear is the path to the dark side."
vader2:"I find your lack of faith disturbing."
leia1:"Help me, Obi-Wan Kenobi. You're my only hope."
han2:"Great, kid! Don't get cocky."
EOF

# Generate Disney quotes data
echo -e "\n${YELLOW}--- Sending Disney Quotes ---${NC}"
cat <<EOF | kafka-console-producer --bootstrap-server $BOOTSTRAP_SERVER --topic disney-quotes --property "parse.key=true" --property "key.separator=:"
timon1:"Hakuna Matata!"
buzz1:"To infinity and beyond!"
walt1:"All our dreams can come true, if we have the courage to pursue them."
mulan1:"The flower that blooms in adversity is the most rare and beautiful of all."
rafiki1:"Oh yes, the past can hurt. But you can either run from it, or learn from it."
dory1:"Just keep swimming!"
dumbo1:"The very things that hold you down are going to lift you up."
snowwhite1:"Remember you're the one who can fill the world with sunshine."
simba1:"Being brave doesn't mean you go looking for trouble."
aladdin1:"Do you trust me?"
EOF

# Generate Arnold Schwarzenegger quotes data
echo -e "\n${YELLOW}--- Sending Arnold Schwarzenegger Quotes ---${NC}"
cat <<EOF | kafka-console-producer --bootstrap-server $BOOTSTRAP_SERVER --topic arnold-swerthenagger-quotes --property "parse.key=true" --property "key.separator=:"
terminator1:"I'll be back."
terminator2:"Hasta la vista, baby."
dutch1:"Get to the choppa!"
kindergarten1:"It's not a tumor!"
predator1:"If it bleeds, we can kill it."
terminator3:"You're terminated!"
arnold1:"Strength does not come from winning. Your struggles develop your strengths."
arnold2:"The mind is the limit. As long as the mind can envision it, you can do it."
arnold3:"Failure is not an option."
conan1:"What is best in life? To crush your enemies, see them driven before you, and hear the lamentations of their women."
EOF

# Generate Basketball quotes (various topics to match pattern)
echo -e "\n${YELLOW}--- Sending Basketball Quotes ---${NC}"
cat <<EOF | kafka-console-producer --bootstrap-server $BOOTSTRAP_SERVER --topic basketball-quotes --property "parse.key=true" --property "key.separator=:"
gretzky1:"You miss 100% of the shots you don't take."
jordan1:"I've failed over and over and over again in my life. And that is why I succeed."
pippen1:"Just play. Have fun. Enjoy the game."
vince1:"It's not whether you get knocked down; it's whether you get up."
kobe1:"The most important thing is to try and inspire people so that they can be great in whatever they want to do."
curry1:"Success is not an accident, success is a choice."
jordan2:"Talent wins games, but teamwork and intelligence win championships."
EOF

# Generate Basketball teams data
echo -e "\n${YELLOW}--- Sending Basketball Teams ---${NC}"
cat <<EOF | kafka-console-producer --bootstrap-server $BOOTSTRAP_SERVER --topic basketball-teams --property "parse.key=true" --property "key.separator=:"
team1:"Los Angeles Lakers"
team2:"Golden State Warriors"
team3:"Chicago Bulls"
team4:"Boston Celtics"
team5:"Miami Heat"
team6:"San Antonio Spurs"
team7:"New York Knicks"
team8:"Houston Rockets"
EOF

# Generate Basketball players data
echo -e "\n${YELLOW}--- Sending Basketball Players ---${NC}"
cat <<EOF | kafka-console-producer --bootstrap-server $BOOTSTRAP_SERVER --topic basketball-players --property "parse.key=true" --property "key.separator=:"
player1:"Michael Jordan"
player2:"LeBron James"
player3:"Kobe Bryant"
player4:"Stephen Curry"
player5:"Magic Johnson"
player6:"Larry Bird"
player7:"Tim Duncan"
player8:"Shaquille O'Neal"
EOF

# Generate Basketball games data
echo -e "\n${YELLOW}--- Sending Basketball Games ---${NC}"
cat <<EOF | kafka-console-producer --bootstrap-server $BOOTSTRAP_SERVER --topic basketball-games --property "parse.key=true" --property "key.separator=:"
game1:"Lakers vs Celtics - Game 7"
game2:"Warriors vs Cavaliers - Finals"
game3:"Bulls vs Jazz - Game 6"
game4:"Heat vs Spurs - Game 6"
game5:"Rockets vs Warriors - Game 7"
EOF

# Generate Basketball scores data
echo -e "\n${YELLOW}--- Sending Basketball Scores ---${NC}"
cat <<EOF | kafka-console-producer --bootstrap-server $BOOTSTRAP_SERVER --topic basketball-scores --property "parse.key=true" --property "key.separator=:"
score1:"Lakers 108 - Celtics 107"
score2:"Warriors 129 - Cavaliers 120"
score3:"Bulls 87 - Jazz 86"
score4:"Heat 103 - Spurs 100"
score5:"Rockets 92 - Warriors 101"
EOF

echo -e "\n${GREEN}=========================================${NC}"
echo -e "${GREEN}✓ Sample data generation complete!      ${NC}"
echo -e "${GREEN}=========================================${NC}"
echo ""
echo "Summary of data sent:"
echo "  • star-wars-quotes: 14 messages"
echo "  • disney-quotes: 10 messages"
echo "  • arnold-swerthenagger-quotes: 11 messages"
echo "  • basketball-quotes: 7 messages"
echo "  • basketball-teams: 8 messages"
echo "  • basketball-players: 8 messages"
echo "  • basketball-games: 5 messages"
echo "  • basketball-scores: 5 messages"
echo ""
echo "Total: 68 messages across 8 topics"
echo ""
echo "Topics matching pattern 'basketball.*':"
echo "  • basketball-quotes ✓"
echo "  • basketball-teams ✓"
echo "  • basketball-players ✓"
echo "  • basketball-games ✓"
echo "  • basketball-scores ✓"
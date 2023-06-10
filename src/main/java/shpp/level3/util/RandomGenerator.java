package shpp.level3.util;

import java.util.List;
import java.util.Random;

public class RandomGenerator {
    private static final Random random = new Random();

    public static Random getRandom(){
        return random;
    }
    protected static final int MAX_LENGTH = 50;

    private RandomGenerator(){
        throw new UnsupportedOperationException();
    }

    public static String generateRandomString() {
        int length  = random.nextInt(MAX_LENGTH);
        return random.ints('a', 'z' + 1)
                .limit(length + 1L)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    public static String getRandomKeyFromKeysList(List<String> list){
        return list.get(RandomGenerator.getRandom().nextInt(list.size()));
    }
}

package shpp.level3.util;

import shpp.level3.model.Product;

import java.util.List;
import java.util.Random;

public class RandomGenerator {
    private static final Random random = new Random();
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
    public static String getRandomStringFromList(List<String> list) {
        Random random = new Random();
        int index = random.nextInt(list.size());
        return list.get(index);
    }

    public static Product getRandomProduct(List<String> productTypes){
        String name = RandomGenerator.generateRandomString();
        String type = RandomGenerator.getRandomStringFromList(productTypes);
        String price = String.format("%.2f", (random.nextDouble() * 100)).replaceAll(",",".");

       return new Product(name, type, price);
    }
}

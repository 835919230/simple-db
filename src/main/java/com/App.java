package com;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        int a = 0;
        for (int i = 0; i < 100; i++) {
            a = a++;
        }
        System.out.println(a);
    }
}

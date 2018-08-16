package com.otherpkg;

public class BooleanOperatorsExample {

	public static void main(String[] args) {
		System.out.println("------- & Operator ------");
		if(check1IsGreaterThan2() & check2IsGreaterThan1())
			System.out.println("1 > 2 : true");
		else
			System.out.println("Output--> (1 > 2) & (2 >1 ) : false");
		
		System.out.println("------- && Operator ------");
		if(check1IsGreaterThan2() && check2IsGreaterThan1())
			System.out.println("1 > 2 : true");
		else
			System.out.println("Output--> (1 > 2) && (2 >1 ) : false");

		System.out.println("------- & Operator ------");
		if(check2IsGreaterThan1() & check1IsGreaterThan2())
			System.out.println("1 > 2 : true");
		else
			System.out.println("Output--> (2 >1 ) & (1 > 2) : false");
		
		System.out.println("------- && Operator ------");
		if(check2IsGreaterThan1() && check1IsGreaterThan2())
			System.out.println("1 > 2 : true");
		else
			System.out.println("Output--> (2 >1 ) && (1 > 2) : false");
	}
	
	public static boolean check1IsGreaterThan2(){
		System.out.println("Inside 1 > 2 --> Returns : "+(1>2));
		return 1>2;
	}

	public static boolean check2IsGreaterThan1(){
		System.out.println("Inside 2 > 1 --> Returns : "+(2>1));
		return 2>1;
	}
}


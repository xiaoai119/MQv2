package com.mq.common.message;

import java.util.HashSet;

/**
 * Created By xfj on 2020/2/5
 */
public class MessageType {
	private static HashSet<Integer> set;
	public static final int ONE_WAY = 0;
	public static final int REPLY_EXPECTED = 1;
	public static final int PULL = 2;
	public static final int ACK = 3;
}

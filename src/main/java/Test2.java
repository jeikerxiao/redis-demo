/**
 * @author : xiao
 * @date : 2018/8/31 下午9:42
 * @description :
 */
public class Test2 {

    public static void main(String[] args) {
        int a = 1;
        int b = 2;
        // 格式2：assert [boolean 表达式] : [错误表达式 （日志）]
        assert a > b : "错误，a不大于b";
    }
}

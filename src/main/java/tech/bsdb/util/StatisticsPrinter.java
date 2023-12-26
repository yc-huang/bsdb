package tech.bsdb.util;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.ArrayList;
import java.util.List;

public class StatisticsPrinter implements SignalHandler {
    private static List<StatisticsItem> items = new ArrayList<>();
    private static SignalHandler originalHandler;
    private static StatisticsPrinter handler = new StatisticsPrinter();

    static {
        originalHandler = Signal.handle(new Signal("INT"), handler);
    }
    public static void addStatistics(StatisticsItem item){
        items.add(item);
    }

    @Override
    public void handle(Signal signal) {
        if (signal.getNumber() == 2) {
            for(StatisticsItem item : items){
                item.showStatistics();
            }
        }
        originalHandler.handle(signal);
    }

    public interface StatisticsItem{
        void showStatistics();
    }
}

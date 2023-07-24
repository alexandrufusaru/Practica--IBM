import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.*;

public class TableDialogg extends JFrame{
    private JTable table;
    private JPanel mainPanel;

    public TableDialogg(){
        setContentPane(mainPanel);
        setTitle("Countries");
        setSize(550,500);
        setLocation(700,250);
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setVisible(true);


                try {
                    Class.forName("oracle.jdbc.driver.OracleDriver");
                    Connection con= DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe","exemplu_user","exemplu_user");
                    Statement st=con.createStatement();
                    String query="select * from countries";
                    ResultSet rs=st.executeQuery(query);
                    ResultSetMetaData rsmd=rs.getMetaData();

                    DefaultTableModel model= (DefaultTableModel) table.getModel();

                    int cols=rsmd.getColumnCount();
                    String[] columnNames=new String[cols];

                    for(int i=0;i<cols;i++)
                        columnNames[i]=rsmd.getColumnName(i+1);
                    model.setColumnIdentifiers(columnNames);

                    String rcc;
                    String scc;
                    int nos;
                    while(rs.next()){
                        rcc=rs.getString(1);
                        scc=rs.getString(2);
                        nos=rs.getInt(3);
                        Object[] row={rcc,scc,nos};
                        model.addRow(row);
                    }
                    st.close();
                    con.close();

                } catch (ClassNotFoundException | SQLException e1) {
                    e1.printStackTrace();
                }

        JTableHeader header= table.getTableHeader();
        Color color=new Color(136, 74, 57);
        header.setBackground(color);
        header.setForeground(Color.WHITE);
        header.setFont(new Font("Myanmar Text",Font.PLAIN,14));
    }

//    public static void main(String[] args) {
//        TableDialogg tableDialogg=new TableDialogg();
//    }
}

<%@page import="javax.naming.InitialContext"%>
<%@page import="javax.naming.Context"%>
<html>
<body>
<h2>Hello World!</h2>
<%
// Получить корневой контекст JNDI
Context initContext = new InitialContext();
// Получить ссылку на класс Avalanche по имени JNDI
Object object = initContext.lookup("java:comp/env/avalanche/gui");
%>
<%= object.getClass().getName() %>
</body>
</html>

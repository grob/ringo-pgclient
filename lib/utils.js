exports.loadJars = () => {
    // add all jar files in jars directory to classpath
    getRepository(module.resolve("../jars/"))
            .getResources()
            .filter(resource => resource.name.endsWith(".jar"))
            .forEach(file => addToClasspath(file));
};

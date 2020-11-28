IMAGE_NAME=$1
echo "Using $IMAGE_NAME as the image name"

# Make the docker image
docker build -t $IMAGE_NAME .
if [ $? -ne 0 ]; then
    echo "Docker build failed"
    exit
fi
docker push $IMAGE_NAME
if [ $? -ne 0 ]; then
    echo "Docker push failed"
    exit
fi
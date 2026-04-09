class kickoff_job:
    def __init__(self, image_id, resource_requirements, image_name):
        self.image_id = image_id
        self.resource_requirements = resource_requirements
        self.image_name = image_name

    def kickoff(self):
        print(f"Kicking off job for image {self.image_id} with resource requirements {self.resource_requirements} and image name {self.image_name}")
        
        